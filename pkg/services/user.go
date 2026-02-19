package services

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/ViktorsBaikers/teldrive/internal/api"
	"github.com/ViktorsBaikers/teldrive/internal/auth"
	"github.com/ViktorsBaikers/teldrive/internal/cache"
	"github.com/ViktorsBaikers/teldrive/internal/tgc"
	"github.com/ViktorsBaikers/teldrive/internal/tgstorage"
	"github.com/ViktorsBaikers/teldrive/pkg/models"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/gotd/contrib/storage"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm/clause"
)

func (a *apiService) UsersAddBots(ctx context.Context, req *api.AddBots) error {
	userID := auth.GetUser(ctx)

	payload := []models.Bot{}
	if len(req.Bots) > 0 {
		for _, token := range req.Bots {
			payload = append(payload, models.Bot{UserId: userID, Token: token})
		}
		if err := a.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&payload).Error; err != nil {
			return err
		}
		var channels []int64
		if err := a.db.Model(&models.Channel{}).Where("user_id = ?", userID).Pluck("channel_id", &channels).Error; err != nil {
			return err
		}
		if len(channels) > 0 {
			for _, channel := range channels {
				a.channelManager.AddBotsToChannel(ctx, userID, channel, req.Bots, false)
			}
		}
		a.cache.Delete(ctx, cache.KeyUserBots(userID))
	}
	return nil

}

func (a *apiService) UsersListChannels(ctx context.Context) ([]api.Channel, error) {

	userId := auth.GetUser(ctx)

	channels := make(map[int64]*api.Channel)

	peerStorage := tgstorage.NewPeerStorage(a.db, cache.KeyPeer(userId))

	iter, err := peerStorage.Iterate(ctx)
	if err != nil {
		return []api.Channel{}, nil
	}
	defer iter.Close()
	for iter.Next(ctx) {
		peer := iter.Value()
		if peer.Channel != nil && peer.Channel.AdminRights.AddAdmins {
			_, exists := channels[peer.Channel.ID]
			if !exists {
				channels[peer.Channel.ID] = &api.Channel{ChannelId: api.NewOptInt64(peer.Channel.ID), ChannelName: peer.Channel.Title}
			}
		}

	}
	res := []api.Channel{}
	for _, channel := range channels {
		res = append(res, *channel)

	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].ChannelName < res[j].ChannelName
	})
	return res, nil
}

func (a *apiService) UsersCreateChannel(ctx context.Context, req *api.Channel) error {
	userID := auth.GetUser(ctx)
	_, err := a.channelManager.CreateNewChannel(ctx, req.ChannelName, userID, false)
	if err != nil {
		return &apiError{err: err}
	}
	return nil
}

func (a *apiService) UsersDeleteChannel(ctx context.Context, params api.UsersDeleteChannelParams) error {
	userId := auth.GetUser(ctx)
	client, _ := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.newMiddlewares(ctx, 5)...)
	channelId, _ := strconv.ParseInt(params.ID, 10, 64)
	peerStorage := tgstorage.NewPeerStorage(a.db, cache.KeyPeer(userId))
	var (
		channel *tg.Channel
		err     error
	)
	err = client.Run(ctx, func(ctx context.Context) error {
		channel, err = tgc.GetChannelFull(ctx, client.API(), channelId)
		if err != nil {
			return err
		}
		_, err = client.API().ChannelsDeleteChannel(ctx, channel.AsInput())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return &apiError{err: err}
	}
	a.db.Where("channel_id = ?", channelId).Delete(&models.Channel{})
	peer := storage.Peer{}
	peer.FromChat(channel)
	peerStorage.Delete(ctx, storage.KeyFromPeer(peer))
	return nil
}

func (a *apiService) UsersSyncChannels(ctx context.Context) error {
	userId := auth.GetUser(ctx)
	peerStorage := tgstorage.NewPeerStorage(a.db, cache.KeyPeer(userId))
	err := peerStorage.Purge(ctx)
	if err != nil {
		return &apiError{err: err}
	}
	collector := storage.CollectPeers(peerStorage)
	client, err := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.newMiddlewares(ctx, 5)...)
	if err != nil {
		return &apiError{err: err}
	}
	err = client.Run(ctx, func(ctx context.Context) error {
		return collector.Dialogs(ctx, query.GetDialogs(client.API()).Iter())
	})
	if err != nil {
		return &apiError{err: err}
	}
	return nil
}

func (a *apiService) UsersListSessions(ctx context.Context) ([]api.UserSession, error) {
	userId := auth.GetUser(ctx)
	return cache.Fetch(ctx, a.cache, cache.KeyUserSessions(userId), 0, func(fetchCtx context.Context) ([]api.UserSession, error) {
		userSession := auth.GetJWTUser(fetchCtx).TgSession
		client, _ := tgc.AuthClient(fetchCtx, &a.cnf.TG, userSession, a.newMiddlewares(fetchCtx, 5)...)
		var (
			auth *tg.AccountAuthorizations
			err  error
		)
		err = client.Run(fetchCtx, func(runCtx context.Context) error {
			auth, err = client.API().AccountGetAuthorizations(runCtx)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil && !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
			return nil, err
		}

		dbSessions := []models.Session{}

		if err = a.db.Where("user_id = ?", userId).Order("created_at DESC").Find(&dbSessions).Error; err != nil {
			return nil, err
		}

		sessionsOut := []api.UserSession{}

		for _, session := range dbSessions {

			s := api.UserSession{Hash: session.Hash,
				CreatedAt: session.CreatedAt.UTC(),
				Current:   session.Session == userSession}

			if auth != nil {
				for _, a := range auth.Authorizations {
					if session.SessionDate == a.DateCreated {
						s.AppName = api.NewOptString(strings.Trim(strings.ReplaceAll(a.AppName, "Telegram", ""), " "))
						s.Location = api.NewOptString(a.Country)
						s.OfficialApp = api.NewOptBool(a.OfficialApp)
						s.Valid = true
						break
					}
				}
			}

			sessionsOut = append(sessionsOut, s)
		}

		return sessionsOut, nil

	})

}

func (a *apiService) UsersProfileImage(ctx context.Context, params api.UsersProfileImageParams) (*api.UsersProfileImageOKHeaders, error) {

	client, err := tgc.AuthClient(ctx, &a.cnf.TG, auth.GetJWTUser(ctx).TgSession, a.newMiddlewares(ctx, 5)...)

	if err != nil {
		return nil, &apiError{err: err}
	}

	res := &api.UsersProfileImageOKHeaders{}

	err = tgc.RunWithAuth(ctx, client, "", func(ctx context.Context) error {
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		peer := self.AsInputPeer()
		if self.Photo == nil {
			return nil
		}
		photo, ok := self.Photo.AsNotEmpty()
		if !ok {
			return errors.New("profile not found")
		}
		photo.GetPersonal()
		location := &tg.InputPeerPhotoFileLocation{Big: false, Peer: peer, PhotoID: photo.PhotoID}
		buff, err := tgc.GetMediaContent(ctx, client.API(), location)
		if err != nil {
			return err
		}
		content := buff.Bytes()
		res.SetCacheControl("public, max-age=86400, must-revalidate")
		res.SetContentLength(int64(len(content)))
		res.SetEtag(fmt.Sprintf("\"%v\"", photo.PhotoID))
		res.SetContentDisposition(fmt.Sprintf("inline; filename=\"%s\"", "profile.jpeg"))
		res.Response = api.UsersProfileImageOK{Data: bytes.NewReader(content)}
		return nil
	})
	if err != nil {
		return nil, &apiError{err: err}
	}
	return res, nil
}

func (a *apiService) UsersRemoveBots(ctx context.Context) error {
	userId := auth.GetUser(ctx)

	if err := a.db.Where("user_id = ?", userId).Delete(&models.Bot{}).Error; err != nil {
		return &apiError{err: err}
	}
	a.cache.Delete(ctx, cache.KeyUserBots(userId))

	return nil
}

func (a *apiService) UsersRemoveBot(ctx context.Context, params api.UsersRemoveBotParams) error {
	userId := auth.GetUser(ctx)
	tokenID := strings.TrimSpace(params.ID)
	if tokenID == "" {
		return &apiError{err: errors.New("invalid bot id"), code: 400}
	}
	if botID, err := strconv.ParseInt(tokenID, 10, 64); err != nil || botID <= 0 {
		return &apiError{err: errors.New("invalid bot id"), code: 400}
	}

	res := a.db.
		Where("user_id = ?", userId).
		Where("token LIKE ?", tokenID+":%").
		Delete(&models.Bot{})
	if res.Error != nil {
		return &apiError{err: res.Error}
	}
	if res.RowsAffected == 0 {
		return &apiError{err: errors.New("bot not found"), code: 404}
	}

	a.cache.Delete(ctx, cache.KeyUserBots(userId))
	return nil
}

func (a *apiService) UsersRemoveSession(ctx context.Context, params api.UsersRemoveSessionParams) error {
	userId := auth.GetUser(ctx)

	session := &models.Session{}

	if err := a.db.Where("user_id = ?", userId).Where("hash = ?", params.ID).First(session).Error; err != nil {
		return &apiError{err: err}
	}

	client, _ := tgc.AuthClient(ctx, &a.cnf.TG, session.Session, a.newMiddlewares(ctx, 5)...)

	client.Run(ctx, func(ctx context.Context) error {
		_, err := client.API().AuthLogOut(ctx)
		if err != nil {
			return err
		}
		return nil
	})

	a.db.Where("user_id = ?", userId).Where("hash = ?", session.Hash).Delete(&models.Session{})
	a.cache.Delete(ctx, cache.KeyUserSessions(userId))

	return nil
}

func (a *apiService) UsersStats(ctx context.Context) (*api.UserConfig, error) {
	userId := auth.GetUser(ctx)
	var (
		channelId int64
		err       error
	)

	channelId, err = a.channelManager.CurrentChannel(ctx, userId)
	if err != nil {
		channelId = 0
	}

	tokens, err := a.channelManager.BotTokens(ctx, userId)

	if err != nil {
		tokens = []string{}
	}
	return &api.UserConfig{Bots: tokens, ChannelId: channelId}, nil
}

func (a *apiService) UsersUpdateChannel(ctx context.Context, req *api.ChannelUpdate) error {
	userId := auth.GetUser(ctx)

	channel := &models.Channel{UserId: userId, Selected: true}

	if req.ChannelId.Value != 0 {
		channel.ChannelId = req.ChannelId.Value
	}
	if req.ChannelName.Value != "" {
		channel.ChannelName = req.ChannelName.Value
	}

	if err := a.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "channel_id"}},
		DoUpdates: clause.Assignments(map[string]any{"selected": true}),
	}).Create(channel).Error; err != nil {
		return &apiError{err: errors.New("failed to update channel")}
	}
	a.db.Model(&models.Channel{}).Where("channel_id != ?", channel.ChannelId).
		Where("user_id = ?", userId).Update("selected", false)

	a.cache.Set(ctx, cache.KeyUserChannel(userId), channel.ChannelId, 0)
	return nil
}

func (a *apiService) UsersBotsHealth(ctx context.Context) (*api.BotHealthResponse, error) {
	if a.botHealth == nil {
		return &api.BotHealthResponse{Bots: []api.BotHealthStatus{}}, nil
	}

	userId := auth.GetUser(ctx)
	tokens, err := a.channelManager.BotTokens(ctx, userId)
	if err != nil {
		return nil, &apiError{err: err}
	}

	stats := a.botHealth.Stats(tokens)
	bots := make([]api.BotHealthStatus, len(stats))
	for i, s := range stats {
		status := api.BotHealthStatus{
			Token:               s.Token,
			Available:           s.Available,
			ConsecutiveFailures: s.ConsecutiveFailures,
			TotalFailures:       s.TotalFailures,
			TotalSuccesses:      s.TotalSuccesses,
			CircuitTrips:        s.CircuitTrips,
		}
		if s.LastError != "" {
			status.LastError = api.NewOptString(s.LastError)
		}
		if !s.OpenUntil.IsZero() {
			status.OpenUntil = api.NewOptDateTime(s.OpenUntil)
		}
		bots[i] = status
	}

	return &api.BotHealthResponse{
		Bots:             bots,
		FailureThreshold: int(a.botHealth.FailureThreshold()),
		CooldownSeconds:  int(a.botHealth.Cooldown().Seconds()),
	}, nil
}

func (a *apiService) UsersBotsDiagnostics(ctx context.Context) (*api.BotDiagnosticsResponse, error) {
	userId := auth.GetUser(ctx)

	tokens, err := a.channelManager.BotTokens(ctx, userId)
	if err != nil {
		return nil, &apiError{err: err}
	}

	// Diagnostics are done against the currently selected channel (if any).
	channelId, err := a.channelManager.CurrentChannel(ctx, userId)
	if err != nil {
		channelId = 0
	}

	out := make([]api.BotDiagnostics, len(tokens))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for i, token := range tokens {
		i, token := i, token
		g.Go(func() error {
			out[i] = a.diagnoseBot(ctx, token, channelId)
			return nil
		})
	}
	_ = g.Wait()

	resp := &api.BotDiagnosticsResponse{Bots: out}
	if channelId != 0 {
		resp.ChannelId = api.NewOptInt64(channelId)
	}
	return resp, nil
}

func (a *apiService) diagnoseBot(ctx context.Context, token string, channelId int64) api.BotDiagnostics {
	tokenID := strings.SplitN(token, ":", 2)[0]
	diag := api.BotDiagnostics{
		TokenId: tokenID,
		Checks:  []api.BotDiagnosticCheck{},
	}

	addCheck := func(name string, ok bool, detail string, action string) {
		check := api.BotDiagnosticCheck{Name: name, Ok: ok}
		if detail != "" {
			check.Detail = api.NewOptString(detail)
		}
		if action != "" {
			check.Action = api.NewOptString(action)
		}
		diag.Checks = append(diag.Checks, check)
	}

	info, err := tgc.GetBotInfo(ctx, a.db, a.cache, &a.cnf.TG, token)
	if err != nil {
		addCheck(
			"token_valid",
			false,
			err.Error(),
			"Verify the token is correct and has not been revoked, then re-add the bot.",
		)
		return diag
	}
	if info != nil && info.UserName != "" {
		diag.UserName = api.NewOptString(info.UserName)
	}
	addCheck("token_valid", true, "Authenticated as bot", "")

	if channelId == 0 {
		addCheck(
			"channel_selected",
			false,
			"No default channel selected",
			"Select a default channel in Settings > Channels.",
		)
		return diag
	}
	addCheck("channel_selected", true, fmt.Sprintf("Channel ID %d", channelId), "")

	middlewares := a.newMiddlewares(ctx, 2)
	botClient, err := tgc.BotClient(ctx, a.db, a.cache, &a.cnf.TG, token, middlewares...)
	if err != nil {
		addCheck("bot_client", false, err.Error(), "Restart the server and check database connectivity.")
		return diag
	}

	var (
		channel *tg.Channel
		member  bool
		admin   bool
		canPost bool
	)

	runErr := tgc.RunWithAuth(ctx, botClient, token, func(ctx context.Context) error {
		var err error
		channel, err = tgc.GetChannelFull(ctx, botClient.API(), channelId)
		if err != nil {
			return err
		}

		participant, err := botClient.API().ChannelsGetParticipant(ctx, &tg.ChannelsGetParticipantRequest{
			Channel:     channel.AsInput(),
			Participant: &tg.InputPeerSelf{},
		})
		if err != nil {
			return err
		}

		switch p := participant.Participant.(type) {
		case *tg.ChannelParticipantCreator:
			member = true
			admin = true
			canPost = true
		case *tg.ChannelParticipantAdmin:
			member = true
			admin = true
			canPost = p.AdminRights.PostMessages
		case *tg.ChannelParticipant:
			member = true
		default:
			member = true
		}
		return nil
	})

	if runErr != nil || channel == nil {
		addCheck(
			"channel_accessible",
			false,
			runErr.Error(),
			"Ensure the channel ID is correct and the bot is added to the channel (preferably as admin).",
		)
		return diag
	}
	addCheck("channel_accessible", true, channel.Title, "")

	if !member {
		addCheck("member", false, "Bot is not a member of the channel", "Add the bot to the channel.")
		return diag
	}
	addCheck("member", true, "Bot is a member of the channel", "")

	if !admin {
		addCheck(
			"admin",
			false,
			"Bot is not an admin in the channel",
			"Grant the bot admin rights (post messages, delete messages) in the target channel.",
		)
		return diag
	}
	addCheck("admin", true, "Bot is an admin in the channel", "")

	if !canPost {
		addCheck("can_post", false, "Bot cannot post messages", "Enable the bot's permission to post messages in the channel.")
	} else {
		addCheck("can_post", true, "Bot can post messages", "")
	}

	return diag
}
