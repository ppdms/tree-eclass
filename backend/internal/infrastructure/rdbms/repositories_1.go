package rdbms

import "tree-eclass/internal/domain/database"

func (p *sqlitePool) Courses() database.Courses {
	return sqliteCourses{db: p}
}

func (p *sqliteTx) Courses() database.Courses {
	return sqliteCourses{db: p}
}

func (p *sqlitePool) Activity() database.Activity {
	return sqliteActivity{db: p}
}

func (p *sqliteTx) Activity() database.Activity {
	return sqliteActivity{db: p}
}

func (p *sqlitePool) Exercises() database.Exercises {
	return sqliteExercises{db: p}
}

func (p *sqliteTx) Exercises() database.Exercises {
	return sqliteExercises{db: p}
}

func (p *sqlitePool) Settings() database.Settings {
	return sqliteSettings{db: p}
}

func (p *sqliteTx) Settings() database.Settings {
	return sqliteSettings{db: p}
}

func (p *sqlitePool) Navigation() database.Navigation {
	return sqliteNavigation{db: p}
}

func (p *sqliteTx) Navigation() database.Navigation {
	return sqliteNavigation{db: p}
}

func (p *sqlitePool) Study() database.Study {
	return sqliteStudy{db: p}
}

func (p *sqliteTx) Study() database.Study {
	return sqliteStudy{db: p}
}

func (p *sqlitePool) Workspace() database.Workspace {
	return sqliteWorkspace{db: p}
}

func (p *sqliteTx) Workspace() database.Workspace {
	return sqliteWorkspace{db: p}
}

func (p *sqlitePool) Annotations() database.Annotations {
	return sqliteAnnotations{db: p}
}

func (p *sqliteTx) Annotations() database.Annotations {
	return sqliteAnnotations{db: p}
}

func (p *sqlitePool) Practice() database.Practice {
	return sqlitePractice{db: p}
}

func (p *sqliteTx) Practice() database.Practice {
	return sqlitePractice{db: p}
}

func (p *sqliteConn) Courses() database.Courses {
	return sqliteCourses{db: p}
}

func (p *sqliteConn) Activity() database.Activity {
	return sqliteActivity{db: p}
}

func (p *sqliteConn) Exercises() database.Exercises {
	return sqliteExercises{db: p}
}

func (p *sqliteConn) Settings() database.Settings {
	return sqliteSettings{db: p}
}

func (p *sqliteConn) Navigation() database.Navigation {
	return sqliteNavigation{db: p}
}

func (p *sqliteConn) Study() database.Study {
	return sqliteStudy{db: p}
}

func (p *sqliteConn) Workspace() database.Workspace {
	return sqliteWorkspace{db: p}
}

func (p *sqliteConn) Annotations() database.Annotations {
	return sqliteAnnotations{db: p}
}

func (p *sqliteConn) Practice() database.Practice {
	return sqlitePractice{db: p}
}
