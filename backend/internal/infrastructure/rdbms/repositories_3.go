package rdbms

import "tree-eclass/internal/domain/database"

func (p *postgresPool) Chat() database.Chat {
	return postgresChat{db: p}
}

func (p *postgresTx) Chat() database.Chat {
	return postgresChat{db: p}
}

func (p *postgresConn) Chat() database.Chat {
	return postgresChat{db: p}
}

func (p *sqlitePool) Chat() database.Chat {
	return sqliteChat{db: p}
}

func (p *sqliteTx) Chat() database.Chat {
	return sqliteChat{db: p}
}

func (p *postgresPool) Discord() database.Discord {
	return postgresDiscord{db: p}
}

func (p *postgresTx) Discord() database.Discord {
	return postgresDiscord{db: p}
}

func (p *postgresConn) Discord() database.Discord {
	return postgresDiscord{db: p}
}

func (p *sqlitePool) Discord() database.Discord {
	return sqliteDiscord{db: p}
}

func (p *sqliteTx) Discord() database.Discord {
	return sqliteDiscord{db: p}
}

func (p *postgresPool) PDFDifferences() database.PDFDifferences {
	return postgresPDFDifferences{db: p}
}

func (p *postgresTx) PDFDifferences() database.PDFDifferences {
	return postgresPDFDifferences{db: p}
}

func (p *postgresConn) PDFDifferences() database.PDFDifferences {
	return postgresPDFDifferences{db: p}
}

func (p *sqlitePool) PDFDifferences() database.PDFDifferences {
	return sqlitePDFDifferences{db: p}
}

func (p *sqliteTx) PDFDifferences() database.PDFDifferences {
	return sqlitePDFDifferences{db: p}
}

func (p *postgresPool) Notifications() database.Notifications {
	return postgresNotifications{db: p}
}

func (p *postgresTx) Notifications() database.Notifications {
	return postgresNotifications{db: p}
}

func (p *postgresConn) Notifications() database.Notifications {
	return postgresNotifications{db: p}
}

func (p *sqlitePool) Notifications() database.Notifications {
	return sqliteNotifications{db: p}
}

func (p *sqliteTx) Notifications() database.Notifications {
	return sqliteNotifications{db: p}
}

func (p *postgresPool) Quota() database.Quota {
	return postgresQuota{db: p}
}

func (p *postgresTx) Quota() database.Quota {
	return postgresQuota{db: p}
}

func (p *postgresConn) Quota() database.Quota {
	return postgresQuota{db: p}
}

func (p *sqlitePool) Quota() database.Quota {
	return sqliteQuota{db: p}
}

func (p *sqliteTx) Quota() database.Quota {
	return sqliteQuota{db: p}
}

func (p *postgresPool) Jobs() database.Jobs {
	return postgresJobs{db: p}
}

func (p *postgresTx) Jobs() database.Jobs {
	return postgresJobs{db: p}
}

func (p *postgresConn) Jobs() database.Jobs {
	return postgresJobs{db: p}
}

func (p *sqlitePool) Jobs() database.Jobs {
	return sqliteJobs{db: p}
}

func (p *sqliteTx) Jobs() database.Jobs {
	return sqliteJobs{db: p}
}

func (p *postgresPool) ObjectCatalog() database.ObjectCatalog {
	return postgresObjectCatalog{db: p}
}

func (p *postgresTx) ObjectCatalog() database.ObjectCatalog {
	return postgresObjectCatalog{db: p}
}

func (p *postgresConn) ObjectCatalog() database.ObjectCatalog {
	return postgresObjectCatalog{db: p}
}

func (p *sqlitePool) ObjectCatalog() database.ObjectCatalog {
	return sqliteObjectCatalog{db: p}
}

func (p *sqliteTx) ObjectCatalog() database.ObjectCatalog {
	return sqliteObjectCatalog{db: p}
}

func (p *postgresPool) Runtime() database.Runtime {
	return postgresRuntime{db: p}
}

func (p *postgresTx) Runtime() database.Runtime {
	return postgresRuntime{db: p}
}

func (p *postgresConn) Runtime() database.Runtime {
	return postgresRuntime{db: p}
}

func (p *sqlitePool) Runtime() database.Runtime {
	return sqliteRuntime{db: p}
}

func (p *sqliteTx) Runtime() database.Runtime {
	return sqliteRuntime{db: p}
}
