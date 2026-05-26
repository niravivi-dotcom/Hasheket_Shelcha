import json

with open('n8n_workflow_feedback_email_automation.json', encoding='utf-8') as f:
    d = json.load(f)

# Build the JS code carefully - no real newlines inside string literals
lines = [
    "const runnerOutput = $('HTTP: Runner /from-api1').first().json;",
    "const cmReports = (runnerOutput.cm_reports || []);",
    "const date = new Date().toLocaleDateString('he-IL');",
    "if (cmReports.length === 0) return [];",
    "return cmReports.map(cm => ({",
    "  json: {",
    "    to_email: cm.email,",
    "    cm_name: cm.name,",
    "    subject: '\u05d3\u05d5\"\u05d7 \u05d4\u05d9\u05d6\u05d5\u05df \u05d7\u05d5\u05d6\u05e8 \u2014 ' + date,",
    r"    body: '\u05e9\u05dc\u05d5\u05dd ' + cm.name + ',\n\n\u05de\u05e6\u05d5\u05e8\u05e3 \u05d3\u05d5\"\u05d7 \u05e8\u05d9\u05e6\u05d4 \u05de\u05ea\u05d0\u05e8\u05d9\u05da ' + date + '.\n\u05d4\u05d3\u05d5\"\u05d7 \u05db\u05d5\u05dc\u05dc \u05d0\u05ea \u05db\u05dc \u05d4\u05e8\u05e9\u05d5\u05de\u05d5\u05ea \u05d4\u05de\u05d9\u05d5\u05e2\u05d3\u05d5\u05ea \u05dc\u05d8\u05d9\u05e4\u05d5\u05dc\u05da.\n\n\u05d1\u05d1\u05e8\u05db\u05d4',",
    "  },",
    "  binary: {",
    "    attachment: {",
    "      data: cm.report_b64,",
    "      mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',",
    r"      fileName: '\u05d3\u05d5\"\u05d7_' + (cm.name || 'CM').replace(/ /g, '_') + '_' + new Date().toISOString().slice(0,10) + '.xlsx',",
    "    }",
    "  }",
    "}));",
]

code = "\n".join(lines)

# Verify no raw newlines inside string literals (only at line boundaries)
print("Code preview:")
for i, line in enumerate(lines, 1):
    print(f"  {i}: {line[:80]}")

for n in d['nodes']:
    if n['id'] == 'cm-split-001':
        n['parameters']['jsCode'] = code
        print("\nFixed cm-split-001")

with open('n8n_workflow_feedback_email_automation.json', 'w', encoding='utf-8') as f:
    json.dump(d, f, ensure_ascii=False, indent=2)
print("Saved.")
