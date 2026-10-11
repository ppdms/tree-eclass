package rdbms

import "tree-eclass/internal/domain/database"

func (p *sqlitePool) Chat() database.Chat {
	return sqliteChat{db: p}
}

func (p *sqliteTx) Chat() database.Chat {
	return sqliteChat{db: p}
}

func (p *sqlitePool) Discord() database.Discord {
	return sqliteDiscord{db: p}
}

func (p *sqliteTx) Discord() database.Discord {
	return sqliteDiscord{db: p}
}

func (p *sqlitePool) PDFDifferences() database.PDFDifferences {
	return sqlitePDFDifferences{db: p}
}

func (p *sqliteTx) PDFDifferences() database.PDFDifferences {
	return sqlitePDFDifferences{db: p}
}

func (p *sqlitePool) Notifications() database.Notifications {
	return sqliteNotifications{db: p}
}

func (p *sqliteTx) Notifications() database.Notifications {
	return sqliteNotifications{db: p}
}

func (p *sqlitePool) Quota() database.Quota {
	return sqliteQuota{db: p}
}

func (p *sqliteTx) Quota() database.Quota {
	return sqliteQuota{db: p}
}

func (p *sqlitePool) Jobs() database.Jobs {
	return sqliteJobs{db: p}
}

func (p *sqliteTx) Jobs() database.Jobs {
	return sqliteJobs{db: p}
}

func (p *sqlitePool) ObjectCatalog() database.ObjectCatalog {
	return sqliteObjectCatalog{db: p}
}

func (p *sqliteTx) ObjectCatalog() database.ObjectCatalog {
	return sqliteObjectCatalog{db: p}
}

func (p *sqlitePool) Runtime() database.Runtime {
	return sqliteRuntime{db: p}
}

func (p *sqliteTx) Runtime() database.Runtime {
	return sqliteRuntime{db: p}
}

func (p *sqliteConn) Chat() database.Chat {
	return sqliteChat{db: p}
}

func (p *sqliteConn) Discord() database.Discord {
	return sqliteDiscord{db: p}
}

func (p *sqliteConn) PDFDifferences() database.PDFDifferences {
	return sqlitePDFDifferences{db: p}
}

func (p *sqliteConn) Notifications() database.Notifications {
	return sqliteNotifications{db: p}
}

func (p *sqliteConn) Quota() database.Quota {
	return sqliteQuota{db: p}
}

func (p *sqliteConn) Jobs() database.Jobs {
	return sqliteJobs{db: p}
}

func (p *sqliteConn) ObjectCatalog() database.ObjectCatalog {
	return sqliteObjectCatalog{db: p}
}

func (p *sqliteConn) Runtime() database.Runtime {
	return sqliteRuntime{db: p}
}
