package server

import "net/http"

func (s *Server) routes() {
	s.courseRoutes()
	s.courseFileRoutes()
	s.coverageRoutes()
	s.navigationRoutes()
	s.studyRoutes()
	s.workspaceRoutes()
	s.practiceRoutes()
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/updates", s.courseUpdates)
	s.fileRoutes()
	s.materialRoutes()
	s.exerciseRoutes()
	s.annotationRoutes()
	s.knowledgeRoutes()
	s.settingsRoutes()
	s.settingsPageRoutes()
	s.conversationRoutes()
	s.syncRoutes()
	s.historyRoutes()
	s.activityRoutes()
	s.mux.HandleFunc("POST /api/v1/study/planner", s.savePlanner)
	s.mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		if err := s.db.Pool.Ping(r.Context()); err != nil {
			writeFailure(w, http.StatusServiceUnavailable, "Database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	s.mux.HandleFunc("GET /api/runtime", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(
			w,
			http.StatusOK,
			map[string]string{"mode": s.config.Mode, "release": s.config.Release, "session": s.config.Session},
		)
	})
}
