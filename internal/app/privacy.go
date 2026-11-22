package app

import (
	"context"
	"time"

	"github.com/overmindv/parker"
	"github.com/overmindv/tasks/internal/adapter/privacyledger"
	"github.com/overmindv/tasks/internal/config"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	"github.com/overmindv/tasks/internal/usecase"
)

func registerPrivacy(app *parker.App, store usecase.PrivacyStore) error {
	cfg, err := config.LoadPrivacy()
	if err != nil {
		return err
	}
	service := &usecase.PrivacyService{PrivacyStore: store, RawRetention: cfg.RawRetention, OutcomeRetention: cfg.OutcomeRetention}
	if cfg.Token != "" {
		service.Ledger = privacyledger.New(cfg.UsersURL, cfg.Token)
	}
	httpapi.RegisterPrivacy(app.HTTP(), service, cfg.ReplayToken, cfg.Token)
	app.AddRunnable("privacy-maintenance", func(ctx context.Context) error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			run, cancel := context.WithTimeout(ctx, 8*time.Second)
			err := service.SyncOnce(run)
			cancel()
			if err != nil && ctx.Err() == nil {
				app.Logger().WarnContext(ctx, "privacy maintenance deferred")
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})

	return nil
}
