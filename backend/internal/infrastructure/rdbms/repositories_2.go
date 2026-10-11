package rdbms

import "tree-eclass/internal/domain/database"

func (p *sqlitePool) Documents() database.Documents {
	return sqliteDocuments{db: p}
}

func (p *sqliteTx) Documents() database.Documents {
	return sqliteDocuments{db: p}
}

func (p *sqlitePool) Indexing() database.Indexing {
	return sqliteIndexing{db: p}
}

func (p *sqliteTx) Indexing() database.Indexing {
	return sqliteIndexing{db: p}
}

func (p *sqlitePool) Community() database.Community {
	return sqliteCommunity{db: p}
}

func (p *sqliteTx) Community() database.Community {
	return sqliteCommunity{db: p}
}

func (p *sqlitePool) DiscordImports() database.DiscordImports {
	return sqliteDiscordImports{db: p}
}

func (p *sqliteTx) DiscordImports() database.DiscordImports {
	return sqliteDiscordImports{db: p}
}

func (p *sqlitePool) Materials() database.Materials {
	return sqliteMaterials{db: p}
}

func (p *sqliteTx) Materials() database.Materials {
	return sqliteMaterials{db: p}
}

func (p *sqlitePool) Objects() database.Objects {
	return sqliteObjects{db: p}
}

func (p *sqliteTx) Objects() database.Objects {
	return sqliteObjects{db: p}
}

func (p *sqlitePool) Analysis() database.Analysis {
	return sqliteAnalysis{db: p}
}

func (p *sqliteTx) Analysis() database.Analysis {
	return sqliteAnalysis{db: p}
}

func (p *sqlitePool) Synthesis() database.Synthesis {
	return sqliteSynthesis{db: p}
}

func (p *sqliteTx) Synthesis() database.Synthesis {
	return sqliteSynthesis{db: p}
}

func (p *sqlitePool) Sync() database.Sync {
	return sqliteSync{db: p}
}

func (p *sqliteTx) Sync() database.Sync {
	return sqliteSync{db: p}
}

func (p *sqliteConn) Documents() database.Documents {
	return sqliteDocuments{db: p}
}

func (p *sqliteConn) Indexing() database.Indexing {
	return sqliteIndexing{db: p}
}

func (p *sqliteConn) Community() database.Community {
	return sqliteCommunity{db: p}
}

func (p *sqliteConn) DiscordImports() database.DiscordImports {
	return sqliteDiscordImports{db: p}
}

func (p *sqliteConn) Materials() database.Materials {
	return sqliteMaterials{db: p}
}

func (p *sqliteConn) Objects() database.Objects {
	return sqliteObjects{db: p}
}

func (p *sqliteConn) Analysis() database.Analysis {
	return sqliteAnalysis{db: p}
}

func (p *sqliteConn) Synthesis() database.Synthesis {
	return sqliteSynthesis{db: p}
}

func (p *sqliteConn) Sync() database.Sync {
	return sqliteSync{db: p}
}
