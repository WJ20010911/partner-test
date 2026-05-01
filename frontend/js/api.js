// API Client
const API_BASE = '';

const api = {
  getToken() { return localStorage.getItem('token'); },

  setToken(token) { localStorage.setItem('token', token); },

  clearToken() { localStorage.removeItem('token'); },

  isLoggedIn() { return !!this.getToken(); },

  async request(method, path, body = null) {
    const headers = { 'Content-Type': 'application/json' };
    const token = this.getToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const opts = { method, headers };
    if (body) opts.body = JSON.stringify(body);

    const res = await fetch(`${API_BASE}${path}`, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Request failed');
    return data;
  },

  get(path) { return this.request('GET', path); },
  post(path, body) { return this.request('POST', path, body); },
  patch(path, body) { return this.request('PATCH', path, body); },

  // Auth
  register(email, password, username) { return this.post('/api/auth/register', { email, password, username }); },
  login(email, password) { return this.post('/api/auth/login', { email, password }); },
  me() { return this.get('/api/auth/me'); },

  // Questions
  getQuestions(count = 20) { return this.get(`/api/questions?count=${count}`); },
  getReplacement(excludeIds = []) { return this.get(`/api/questions/replacement?exclude=${excludeIds.join(',')}`); },
  submitQuestion(data) { return this.post('/api/questions', data); },
  getPendingQuestions() { return this.get('/api/questions/pending'); },
  getAllQuestions() { return this.get('/api/questions/all'); },
  reviewQuestion(id, status) { return this.patch(`/api/questions/${id}`, { status }); },
  getComplaints() { return this.get('/api/questions/complaints'); },

  deleteQuestion(id) { return this.request('DELETE', `/api/questions/${id}`); },

  // Admin
  adminAuth(password) { return this.post('/api/admin/auth', { password }); },
  adminStats() { return this.get('/api/admin/stats'); },
  adminScoreDistribution() { return this.get('/api/admin/score-distribution'); },
  adminTestTrend() { return this.get('/api/admin/test-trend'); },
  adminQuestionStats() { return this.get('/api/admin/question-stats'); },
  adminUsers() { return this.get('/api/admin/users'); },
  adminBanUser(id, banned = 1) { return this.patch(`/api/admin/users/${id}/ban`, { banned }); },
  adminDeleteUser(id) { return this.request('DELETE', `/api/admin/users/${id}`); },
  adminTags() { return this.get('/api/admin/tags'); },
  adminExport() { return this.get('/api/admin/export'); },
  adminLogs() { return this.get('/api/admin/logs'); },
  adminChangePassword(oldPw, newPw) { return this.post('/api/admin/change-password', { old_password: oldPw, new_password: newPw }); },
  adminConfig() { return this.get('/api/admin/config'); },
  checkQuestionCounts() { return this.get('/api/admin/question-counts'); },
  questionBankHistory() { return this.get('/api/admin/question-bank-history'); },
  batchInsertTestQuestions() { return this.post('/api/admin/batch-insert-test', {}); },
  updateQuestion(id, data) { return this.patch(`/api/questions/${id}/edit`, data); },

  // Contributors
  getContributors() { return this.get('/api/contributors'); },

  // Public stats
  getPublicStats() { return this.get('/api/public-stats'); },

  // Tester nickname
  setTesterNickname(token, nickname) { return this.post('/api/tester/nickname', { token, nickname }); },

  // Admin delete with password
  adminDeleteQuestion(id, password) { return this.post('/api/admin/questions/delete', { id, password }); },
  adminBatchDelete(ids, password) { return this.post('/api/admin/questions/batch-delete', { ids, password }); },
  adminBatchSetTimeLimit(ids, timeLimit, password) { return this.post('/api/admin/questions/batch-set-timelimit', { ids, time_limit: timeLimit, password }); },

  // Test
  submitTest(answers) { return this.post('/api/test/submit', { answers }); },
  verifyRecord(id) { return this.get(`/api/test/verify/${id}`); },
  verifyByToken(token) { return this.get(`/api/test/verify-token?token=${encodeURIComponent(token)}`); },
  recordSkip(questionId, reason) { return this.post('/api/test/skip', { question_id: questionId, reason }); },
  getRecord(id) { return this.get(`/api/test/${id}`); },
};
