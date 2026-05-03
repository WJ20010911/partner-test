#!/usr/bin/env python3
"""Transform seed questions: scores 0-3→1-5, weights 1.5→2.0, 2.0→3.0."""
import json, sys, re

sys.path.insert(0, 'backend')
from seed_questions import QUESTIONS_JSON

questions = json.loads(QUESTIONS_JSON)

score_map = {0: 0, 1: 2, 2: 3, 3: 5}
weight_map = {1.5: 2.0, 2.0: 3.0}

for q in questions:
    # Transform option scores
    for opt in q['options']:
        old = opt['score']
        opt['score'] = score_map.get(old, old)
    # Transform weights
    w = q.get('weight', 1.0)
    q['weight'] = weight_map.get(w, w)

# Read the original seed_questions.py
with open('backend/seed_questions.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the QUESTIONS_JSON content
new_json = json.dumps(questions, ensure_ascii=False, indent=2)
# Find the JSON block boundaries
start = content.index("QUESTIONS_JSON = r'''")
end = content.index("'''", start + len("QUESTIONS_JSON = r'''"))
new_content = content[:start] + "QUESTIONS_JSON = r'''\n" + new_json + "\n'''" + content[end+3:]

with open('backend/seed_questions.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("seed_questions.py 已更新")
print(f"题目数: {len(questions)}")
# Verify
for q in questions:
    scores = [o['score'] for o in q['options']]
    if min(scores) < 1 or max(scores) > 5:
        print(f"  WARNING: {q['content'][:30]}... scores={scores}")
    w = q.get('weight', 1.0)
    if w not in (1.0, 2.0, 3.0):
        print(f"  WARNING: {q['content'][:30]}... weight={w}")
print("校验完成")

# Calculate new worst-case
total_best = sum(1 * min(q.get('weight', 1.0) for _ in [q]) for q in questions)
# Actually let's simulate properly
import random
random.seed(42)
worst_scores = []
for _ in range(10000):
    selected = random.sample(questions, 12)
    penalty = sum(5 * q.get('weight', 3.0) for q in selected)
    worst_scores.append(penalty)
print(f"\n新评分模拟：所有选项全选最差 (5分)")
print(f"  10000次随机抽12题，最差得分范围: {min(worst_scores):.0f} ~ {max(worst_scores):.0f}")
print(f"  平均最差得分: {sum(worst_scores)/len(worst_scores):.0f}")
# Best case
best_scores = []
for _ in range(10000):
    selected = random.sample(questions, 12)
    penalty = sum(0 * q.get('weight', 1.0) for q in selected)
    best_scores.append(penalty)
print(f"\n所有选项全选最佳 (0分)")
print(f"  10000次随机抽12题，最佳得分范围: {min(best_scores):.0f} ~ {max(best_scores):.0f}")
print(f"  平均最佳得分: {sum(best_scores)/len(best_scores):.0f}")