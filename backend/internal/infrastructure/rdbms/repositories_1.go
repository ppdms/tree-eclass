package rdbms

import "tree-eclass/internal/domain/database"

func (p *postgresPool) Courses() database.Courses {
	return postgresCourses{db: p}
}

func (p *postgresTx) Courses() database.Courses {
	return postgresCourses{db: p}
}

func (p *postgresConn) Courses() database.Courses {
	return postgresCourses{db: p}
}

func (p *sqlitePool) Courses() database.Courses {
	return sqliteCourses{db: p}
}

func (p *sqliteTx) Courses() database.Courses {
	return sqliteCourses{db: p}
}

func (p *postgresPool) Activity() database.Activity {
	return postgresActivity{db: p}
}

func (p *postgresTx) Activity() database.Activity {
	return postgresActivity{db: p}
}

func (p *postgresConn) Activity() database.Activity {
	return postgresActivity{db: p}
}

func (p *sqlitePool) Activity() database.Activity {
	return sqliteActivity{db: p}
}

func (p *sqliteTx) Activity() database.Activity {
	return sqliteActivity{db: p}
}

func (p *postgresPool) Exercises() database.Exercises {
	return postgresExercises{db: p}
}

func (p *postgresTx) Exercises() database.Exercises {
	return postgresExercises{db: p}
}

func (p *postgresConn) Exercises() database.Exercises {
	return postgresExercises{db: p}
}

func (p *sqlitePool) Exercises() database.Exercises {
	return sqliteExercises{db: p}
}

func (p *sqliteTx) Exercises() database.Exercises {
	return sqliteExercises{db: p}
}

func (p *postgresPool) Settings() database.Settings {
	return postgresSettings{db: p}
}

func (p *postgresTx) Settings() database.Settings {
	return postgresSettings{db: p}
}

func (p *postgresConn) Settings() database.Settings {
	return postgresSettings{db: p}
}

func (p *sqlitePool) Settings() database.Settings {
	return sqliteSettings{db: p}
}

func (p *sqliteTx) Settings() database.Settings {
	return sqliteSettings{db: p}
}

func (p *postgresPool) Navigation() database.Navigation {
	return postgresNavigation{db: p}
}

func (p *postgresTx) Navigation() database.Navigation {
	return postgresNavigation{db: p}
}

func (p *postgresConn) Navigation() database.Navigation {
	return postgresNavigation{db: p}
}

func (p *sqlitePool) Navigation() database.Navigation {
	return sqliteNavigation{db: p}
}

func (p *sqliteTx) Navigation() database.Navigation {
	return sqliteNavigation{db: p}
}

func (p *postgresPool) Study() database.Study {
	return postgresStudy{db: p}
}

func (p *postgresTx) Study() database.Study {
	return postgresStudy{db: p}
}

func (p *postgresConn) Study() database.Study {
	return postgresStudy{db: p}
}

func (p *sqlitePool) Study() database.Study {
	return sqliteStudy{db: p}
}

func (p *sqliteTx) Study() database.Study {
	return sqliteStudy{db: p}
}

func (p *postgresPool) Workspace() database.Workspace {
	return postgresWorkspace{db: p}
}

func (p *postgresTx) Workspace() database.Workspace {
	return postgresWorkspace{db: p}
}

func (p *postgresConn) Workspace() database.Workspace {
	return postgresWorkspace{db: p}
}

func (p *sqlitePool) Workspace() database.Workspace {
	return sqliteWorkspace{db: p}
}

func (p *sqliteTx) Workspace() database.Workspace {
	return sqliteWorkspace{db: p}
}

func (p *postgresPool) Annotations() database.Annotations {
	return postgresAnnotations{db: p}
}

func (p *postgresTx) Annotations() database.Annotations {
	return postgresAnnotations{db: p}
}

func (p *postgresConn) Annotations() database.Annotations {
	return postgresAnnotations{db: p}
}

func (p *sqlitePool) Annotations() database.Annotations {
	return sqliteAnnotations{db: p}
}

func (p *sqliteTx) Annotations() database.Annotations {
	return sqliteAnnotations{db: p}
}

func (p *postgresPool) Practice() database.Practice {
	return postgresPractice{db: p}
}

func (p *postgresTx) Practice() database.Practice {
	return postgresPractice{db: p}
}

func (p *postgresConn) Practice() database.Practice {
	return postgresPractice{db: p}
}

func (p *sqlitePool) Practice() database.Practice {
	return sqlitePractice{db: p}
}

func (p *sqliteTx) Practice() database.Practice {
	return sqlitePractice{db: p}
}
