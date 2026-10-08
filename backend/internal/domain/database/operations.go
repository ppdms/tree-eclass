package database

// Operations is the transaction-composable family of typed persistence ports.
// Each accessor uses the same backing store or transaction; no implicit commits.
type Operations interface {
	Courses() Courses
	Activity() Activity
	Exercises() Exercises
	Settings() Settings
	Navigation() Navigation
	Study() Study
	Workspace() Workspace
	Annotations() Annotations
	Practice() Practice
	Documents() Documents
	Indexing() Indexing
	Community() Community
	DiscordImports() DiscordImports
	Materials() Materials
	Objects() Objects
	Analysis() Analysis
	Synthesis() Synthesis
	Sync() Sync
	Chat() Chat
	Discord() Discord
	PDFDifferences() PDFDifferences
	Notifications() Notifications
	Quota() Quota
	Jobs() Jobs
	ObjectCatalog() ObjectCatalog
	Runtime() Runtime
}
