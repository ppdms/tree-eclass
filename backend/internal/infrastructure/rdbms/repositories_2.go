package rdbms

import "tree-eclass/internal/domain/database"

func (p *postgresPool) Documents() database.Documents {
	return postgresDocuments{db: p}
}

func (p *postgresTx) Documents() database.Documents {
	return postgresDocuments{db: p}
}

func (p *postgresConn) Documents() database.Documents {
	return postgresDocuments{db: p}
}

func (p *sqlitePool) Documents() database.Documents {
	return sqliteDocuments{db: p}
}

func (p *sqliteTx) Documents() database.Documents {
	return sqliteDocuments{db: p}
}

func (p *postgresPool) Indexing() database.Indexing {
	return postgresIndexing{db: p}
}

func (p *postgresTx) Indexing() database.Indexing {
	return postgresIndexing{db: p}
}

func (p *postgresConn) Indexing() database.Indexing {
	return postgresIndexing{db: p}
}

func (p *sqlitePool) Indexing() database.Indexing {
	return sqliteIndexing{db: p}
}

func (p *sqliteTx) Indexing() database.Indexing {
	return sqliteIndexing{db: p}
}

func (p *postgresPool) Community() database.Community {
	return postgresCommunity{db: p}
}

func (p *postgresTx) Community() database.Community {
	return postgresCommunity{db: p}
}

func (p *postgresConn) Community() database.Community {
	return postgresCommunity{db: p}
}

func (p *sqlitePool) Community() database.Community {
	return sqliteCommunity{db: p}
}

func (p *sqliteTx) Community() database.Community {
	return sqliteCommunity{db: p}
}

func (p *postgresPool) DiscordImports() database.DiscordImports {
	return postgresDiscordImports{db: p}
}

func (p *postgresTx) DiscordImports() database.DiscordImports {
	return postgresDiscordImports{db: p}
}

func (p *postgresConn) DiscordImports() database.DiscordImports {
	return postgresDiscordImports{db: p}
}

func (p *sqlitePool) DiscordImports() database.DiscordImports {
	return sqliteDiscordImports{db: p}
}

func (p *sqliteTx) DiscordImports() database.DiscordImports {
	return sqliteDiscordImports{db: p}
}

func (p *postgresPool) Materials() database.Materials {
	return postgresMaterials{db: p}
}

func (p *postgresTx) Materials() database.Materials {
	return postgresMaterials{db: p}
}

func (p *postgresConn) Materials() database.Materials {
	return postgresMaterials{db: p}
}

func (p *sqlitePool) Materials() database.Materials {
	return sqliteMaterials{db: p}
}

func (p *sqliteTx) Materials() database.Materials {
	return sqliteMaterials{db: p}
}

func (p *postgresPool) Objects() database.Objects {
	return postgresObjects{db: p}
}

func (p *postgresTx) Objects() database.Objects {
	return postgresObjects{db: p}
}

func (p *postgresConn) Objects() database.Objects {
	return postgresObjects{db: p}
}

func (p *sqlitePool) Objects() database.Objects {
	return sqliteObjects{db: p}
}

func (p *sqliteTx) Objects() database.Objects {
	return sqliteObjects{db: p}
}

func (p *postgresPool) Analysis() database.Analysis {
	return postgresAnalysis{db: p}
}

func (p *postgresTx) Analysis() database.Analysis {
	return postgresAnalysis{db: p}
}

func (p *postgresConn) Analysis() database.Analysis {
	return postgresAnalysis{db: p}
}

func (p *sqlitePool) Analysis() database.Analysis {
	return sqliteAnalysis{db: p}
}

func (p *sqliteTx) Analysis() database.Analysis {
	return sqliteAnalysis{db: p}
}

func (p *postgresPool) Synthesis() database.Synthesis {
	return postgresSynthesis{db: p}
}

func (p *postgresTx) Synthesis() database.Synthesis {
	return postgresSynthesis{db: p}
}

func (p *postgresConn) Synthesis() database.Synthesis {
	return postgresSynthesis{db: p}
}

func (p *sqlitePool) Synthesis() database.Synthesis {
	return sqliteSynthesis{db: p}
}

func (p *sqliteTx) Synthesis() database.Synthesis {
	return sqliteSynthesis{db: p}
}

func (p *postgresPool) Sync() database.Sync {
	return postgresSync{db: p}
}

func (p *postgresTx) Sync() database.Sync {
	return postgresSync{db: p}
}

func (p *postgresConn) Sync() database.Sync {
	return postgresSync{db: p}
}

func (p *sqlitePool) Sync() database.Sync {
	return sqliteSync{db: p}
}

func (p *sqliteTx) Sync() database.Sync {
	return sqliteSync{db: p}
}
