#!/usr/bin/env python3
"""Generate frontend/test.html for Partner_Text_Pro"""
import sys

html = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>伴侣文本 Pro - 测试</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%); min-height: 100vh; color: #e0e0e0; }
.container { max-width: 900px; margin: 0 auto; padding: 30px 20px; }
h1 { text-align: center; font-size: 2.2em; margin-bottom: 8px; background: linear-gradient(90deg, #f7971e, #ffd200); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }
.subtitle { text-align: center; color: #aab; margin-bottom: 30px; font-size: 1.05em; }
.card { background: rgba(255,255,255,0.06); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.1); border-radius: 16px; padding: 24px 28px; margin-bottom: 20px; }
.card h2 { font-size: 1.2em; margin-bottom: 10px; color: #ffd200; }
.info-row { display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 12px; align-items: center; }
.info-item { background: rgba(255,255,255,0.05); border-radius: 10px; padding: 10px 16px; }
.info-label { font-size: 0.8em; color: #999; text-transform: uppercase; letter-spacing: 1px; }
.info-value { font-size: 1.1em; font-weight: 600; }
.question-box { background: rgba(255,255,255,0.08); border-radius: 12px; padding: 20px 24px; margin: 15px 0; min-height: 80px; font-size: 1.1em; line-height: 1.7; }
.answers { display: flex; flex-direction: column; gap: 10px; margin-top: 15px; }
.answer-btn { display: flex; align-items: center; gap: 12px; padding: 14px 18px; border: 2px solid rgba(255,255,255,0.2); border-radius: 12px; background: rgba(255,255,255,0.04); color: #e0e0e0; cursor: pointer; font-size: 1.05em; transition: all 0.2s; text-align: left; }
.answer-btn:hover:not(:disabled) { background: rgba(255,210,0,0.1); border-color: #ffd200; transform: translateX(4px); }
.answer-btn.correct { background: rgba(76,175,80,0.25); border-color: #4caf50; }
.answer-btn.wrong { background: rgba(244,67,54,0.25); border-color: #f44336; }
.answer-btn:disabled { cursor: not-allowed; opacity: 0.7; }
.answer-btn .badge { font-weight: 700; font-size: 0.85em; background: rgba(255,255,255,0.15); border-radius: 50%; width: 32px; height: 32px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
.feedback { margin-top: 15px; padding: 14px 18px; border-radius: 10px; font-weight: 600; display: none; }
.feedback.show { display: block; }
.feedback.correct-fb { background: rgba(76,175,80,0.2); color: #81c784; }
.feedback.wrong-fb { background: rgba(244,67,54,0.2); color: #ef9a9a; }
.timer-bar { height: 6px; border-radius: 3px; background: rgba(255,255,255,0.1); margin-bottom: 20px; overflow: hidden; }
.timer-fill { height: 100%; border-radius: 3px; transition: width 1s linear; background: linear-gradient(90deg, #4caf50, #ffd200, #f44336); }
.timer-text { text-align: center; font-size: 0.9em; color: #aab; margin-bottom: 5px; }
.btn { padding: 12px 28px; border: none; border-radius: 10px; cursor: pointer; font-size: 1em; font-weight: 600; transition: all 0.2s; }
.btn-primary { background: linear-gradient(135deg, #f7971e, #ffd200); color: #1a1a2e; }
.btn-primary:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(247,151,30,0.3); }
.btn-secondary { background: rgba(255,255,255,0.1); color: #e0e0e0; border: 1px solid rgba(255,255,255,0.2); }
.btn-danger { background: #c62828; color: #fff; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.hidden { display: none !important; }
.loading { text-align: center; padding: 60px 20px; }
.spinner { border: 4px solid rgba(255,255,255,0.1); border-left-color: #ffd200; border-radius: 50%; width: 40px; height: 40px; animation: spin 0.8s linear infinite; margin: 0 auto 15px; }
@keyframes spin { to { transform: rotate(360deg); } }
.error-msg { color: #ef5350; background: rgba(244,67,54,0.15); padding: 12px 18px; border-radius: 10px; margin: 10px 0; }
.result-summary { text-align: center; padding: 30px 20px; }
.result-summary .big-score { font-size: 3.5em; font-weight: 800; background: linear-gradient(90deg, #f7971e, #ffd200); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
.result-summary .grade { font-size: 1.3em; margin: 8px 0; }
.perf-stats { display: flex; flex-wrap: wrap; gap: 15px; justify-content: center; margin: 20px 0; }
.perf-stat { background: rgba(255,255,255,0.06); border-radius: 12px; padding: 14px 20px; text-align: center; min-width: 100px; }
.perf-stat .val { font-size: 1.5em; font-weight: 700; color: #ffd200; }
.perf-stat .lbl { font-size: 0.8em; color: #999; margin-top: 3px; }
.test-id-display { font-size: 0.78em; color: #666; text-align: center; margin-top: 5px; word-break: break-all; }
</style>
</head>
<body>
<div class="container">
  <h1>伴侣文本 Pro</h1>
  <div class="subtitle">深度阅读理解 · 计时挑战</div>
  <div id="loadingArea" class="loading"><div class="spinner"></div><p>正在加载测试数据…</p></div>
  <div id="testArea" class="hidden"></div>
  <div id="resultArea" class="hidden"></div>
  <div id="errorArea" class="hidden"></div>
</div>
'''

js = r'''<script>
// ========== 状态 ==========
let state = {
  testId: null,
  uid: null,
  passage: null,
  questions: [],
  currentIndex: 0,
  answers: [],
  questionStartTime: 0,
  questionTimeLog: []
};

// ========== 初始化 ==========
async function init() {
  const params = new URLSearchParams(window.location.search);
  state.testId = params.get('id');
  state.uid = params.get('uid');

  if (!state.testId || !state.uid) {
    showError('缺少参数：id 或 uid。请使用管理员提供的链接进入测试。');
    return;
  }

  try {
    const resp = await fetch('/api/tests/' + state.testId + '/questions?uid=' + encodeURIComponent(state.uid));
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || '加载失败 (HTTP ' + resp.status + ')');
    }
    const data = await resp.json();
    state.passage = data.passage;
    state.questions = data.questions;
    state.answers = new Array(data.questions.length).fill(null);

    if (!state.questions || state.questions.length === 0) {
      showError('该测试尚未配置题目。');
      return;
    }

    document.getElementById('loadingArea').classList.add('hidden');
    document.getElementById('testArea').classList.remove('hidden');
    renderQuestion();
  } catch (e) {
    showError(e.message || '加载测试失败，请检查链接或刷新重试。');
  }
}

// ========== 渲染题目 ==========
function renderQuestion() {
  const q = state.questions[state.currentIndex];
  const total = state.questions.length;
  const percent = Math.round((state.currentIndex / total) * 100);

  state.questionStartTime = Date.now();

  const labels = ['A', 'B', 'C', 'D'];
  const answersHtml = q.options.map(function(opt, i) {
    var text = typeof opt === 'object' ? (opt.text || '') : opt;
    return '<button class="answer-btn" data-index="' + i + '" onclick="selectAnswer(' + i + ', this)">' +
           '<span class="badge">' + labels[i] + '</span>' +
           '<span>' + escapeHtml(text) + '</span></button>';
  }).join('');

  const html =
    '<div class="timer-bar"><div class="timer-fill" style="width:' + percent + '%"></div></div>' +
    '<div class="timer-text">第 ' + (state.currentIndex + 1) + ' / ' + total + ' 题</div>' +
    '<div class="card">' +
      '<h2>阅读材料</h2>' +
      '<div class="question-box">' + escapeHtml(state.passage) + '</div>' +
    '</div>' +
    '<div class="card">' +
      '<h2>第 ' + (state.currentIndex + 1) + ' 题</h2>' +
      '<div class="question-box">' + escapeHtml(q.question) + '</div>' +
      '<div class="answers">' + answersHtml + '</div>' +
      '<div id="feedback" class="feedback"></div>' +
      '<div style="margin-top:18px;display:flex;gap:10px;flex-wrap:wrap;">' +
        (state.currentIndex > 0 ? '<button class="btn btn-secondary" onclick="prevQuestion()">上一题</button>' : '') +
        (state.currentIndex < total - 1 ? '<button class="btn btn-primary" id="nextBtn" style="display:none;" onclick="nextQuestion()">下一题</button>' : '') +
        (state.currentIndex === total - 1 ? '<button class="btn btn-primary" id="submitBtn" style="display:none;" onclick="submitTest()">提交答卷</button>' : '') +
      '</div>' +
    '</div>';

  document.getElementById('testArea').innerHTML = html;
}

// ========== 选择答案 ==========
function findCorrectIndex(options) {
  var minScore = Infinity, minIdx = 0;
  for (var i = 0; i < options.length; i++) {
    var score = typeof options[i] === 'object' ? (options[i].score != null ? options[i].score : 5) : (i === 0 ? 0 : 5);
    if (score < minScore) { minScore = score; minIdx = i; }
  }
  return minIdx;
}

function selectAnswer(index, btn) {
  var q = state.questions[state.currentIndex];
  var correctIdx = q.correct_index != null ? q.correct_index : findCorrectIndex(q.options);
  var elapsed = (Date.now() - state.questionStartTime) / 1000;

  // Log timing for this question
  state.questionTimeLog.push({
    question_index: state.currentIndex,
    question_id: q.id,
    time_taken: parseFloat(elapsed.toFixed(2)),
    answer_chosen: index
  });

  // 禁用所有按钮
  var buttons = document.querySelectorAll('.answer-btn');
  buttons.forEach(function(b) { b.disabled = true; });

  // 高亮正确/错误
  if (index === correctIdx) {
    btn.classList.add('correct');
    state.answers[state.currentIndex] = index;
  } else {
    btn.classList.add('wrong');
    buttons[correctIdx].classList.add('correct');
    state.answers[state.currentIndex] = index;
  }

  // 显示反馈
  var fb = document.getElementById('feedback');
  fb.classList.add('show');
  if (index === correctIdx) {
    fb.classList.add('correct-fb');
    fb.textContent = '✓ 正确！';
  } else {
    fb.classList.add('wrong-fb');
    fb.textContent = '✗ 错误。正确答案是 ' + ['A','B','C','D'][correctIdx] + '。';
  }

  // 显示下一题/提交按钮
  var nextBtn = document.getElementById('nextBtn');
  var submitBtn = document.getElementById('submitBtn');
  if (nextBtn) nextBtn.style.display = '';
  if (submitBtn) submitBtn.style.display = '';
}

// ========== 导航 ==========
function prevQuestion() {
  if (state.currentIndex > 0) {
    state.currentIndex--;
    renderQuestion();
  }
}

function nextQuestion() {
  if (state.currentIndex < state.questions.length - 1) {
    state.currentIndex++;
    renderQuestion();
  }
}

// ========== 提交测试 ==========
async function submitTest() {
  var unanswered = state.questionTimeLog.length;
  // Check if any question was answered
  var answeredCount = state.answers.filter(function(a) { return a !== null; }).length;
  if (answeredCount === 0 && !confirm('你还没有作答任何题目，确定要提交吗？')) return;

  var totalTime = state.questionTimeLog.reduce(function(sum, entry) { return sum + entry.time_taken; }, 0);

  try {
    var payload = {
      test_id: state.testId,
      uid: state.uid,
      answers: state.questionTimeLog.map(function(entry) {
        return {
          question_id: entry.question_id,
          answer_chosen: entry.answer_chosen,
          time_taken: entry.time_taken
        };
      }),
      total_time: parseFloat(totalTime.toFixed(2))
    };

    var resp = await fetch('/api/tests/' + state.testId + '/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!resp.ok) {
      var err = await resp.json().catch(function() { return {}; });
      throw new Error(err.detail || '提交失败 (HTTP ' + resp.status + ')');
    }

    var result = await resp.json();

    // Show result
    document.getElementById('testArea').classList.add('hidden');
    document.getElementById('resultArea').classList.remove('hidden');
    renderResult(result);
  } catch (e) {
    alert('提交失败：' + (e.message || '未知错误'));
  }
}

// ========== 渲染结果 ==========
function renderResult(result) {
  var score = result.score;
  var total = result.total_questions;
  var pct = total > 0 ? Math.round((score / total) * 100) : 0;
  var incorrect = total - score;
  var avgTime = result.total_time && total > 0 ? (result.total_time / total).toFixed(1) : 'N/A';

  var grade = 'F';
  if (pct >= 90) grade = 'S';
  else if (pct >= 80) grade = 'A';
  else if (pct >= 70) grade = 'B';
  else if (pct >= 60) grade = 'C';
  else if (pct >= 40) grade = 'D';

  var html =
    '<div class="result-summary">' +
      '<div class="big-score">' + pct + '%</div>' +
      '<div class="grade">评级：<strong>' + grade + '</strong></div>' +
      '<div class="perf-stats">' +
        '<div class="perf-stat"><div class="val">' + score + ' / ' + total + '</div><div class="lbl">正确</div></div>' +
        '<div class="perf-stat"><div class="val">' + incorrect + '</div><div class="lbl">错误</div></div>' +
        '<div class="perf-stat"><div class="val">' + avgTime + 's</div><div class="lbl">平均用时/题</div></div>' +
        '<div class="perf-stat"><div class="val">' + (result.total_time || 0).toFixed(1) + 's</div><div class="lbl">总用时</div></div>' +
      '</div>' +
    '</div>' +
    '<div class="test-id-display">测试ID：' + escapeHtml(state.testId) + '</div>' +
    '<div style="text-align:center;margin-top:25px;">' +
      '<button class="btn btn-primary" onclick="location.reload()">重新测试</button>' +
    '</div>';

  document.getElementById('resultArea').innerHTML = html;
}

// ========== 工具函数 ==========
function escapeHtml(str) {
  if (!str) return '';
  var div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function showError(msg) {
  document.getElementById('loadingArea').classList.add('hidden');
  document.getElementById('testArea').classList.add('hidden');
  var errDiv = document.getElementById('errorArea');
  errDiv.classList.remove('hidden');
  errDiv.innerHTML = '<div class="card"><div class="error-msg">' + escapeHtml(msg) + '</div></div>';
}

// ========== 启动 ==========
init();
</script>
</body>
</html>'''

with open('/Users/lupin/partner-test/test_pro.html', 'w', encoding='utf-8') as f:
    f.write(html)
    f.write(js)

print('Written successfully, size:', len(html) + len(js))