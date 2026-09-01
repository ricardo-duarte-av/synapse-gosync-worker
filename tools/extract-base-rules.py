import re, json, sys

raw = open('/home/daedric/synapse/rust/src/push/base_rules.rs').read()
# Single-rule groups are written inline as `= &[PushRule {`; split them onto
# their own line so every group has the same shape.
raw = raw.replace('= &[PushRule {', '= &[\nPushRule {').replace('\n}];', '\n},\n];')
# Some fields wrap onto their own line:
#     rule_id: Cow::Borrowed(
#         "global/underride/...",
#     ),
# Collapse those so a single-line field regex still sees the value. Missing
# this silently DROPPED six rules, because the rule_id came back empty and the
# rule was skipped.
raw = re.sub(r'Cow::Borrowed\(\s*\n\s*(r?"(?:[^"\\\\]|\\\\.)*")\s*,?\s*\n\s*\)', r'Cow::Borrowed(\1)', raw)
src = raw.split('\n')

bounds = [(m.group(1), i) for i, line in enumerate(src)
          for m in [re.match(r'pub const (BASE_\w+): &\[PushRule\] = &\[', line)] if m]
groups_src = {}
for name, start in bounds:
    end = next(j for j in range(start + 1, len(src)) if src[j].startswith('];'))
    groups_src[name] = '\n'.join(src[start:end + 1])

ACTIONS = {'Action::Notify': 'notify',
           'HIGHLIGHT_FALSE_ACTION': {"set_tweak": "highlight", "value": False},
           'HIGHLIGHT_ACTION': {"set_tweak": "highlight"},
           'SOUND_ACTION': {"set_tweak": "sound", "value": "default"},
           'RING_ACTION': {"set_tweak": "sound", "value": "ring"}}
PT = {'EventMatchPatternType::UserId': 'user_id',
      'EventMatchPatternType::UserLocalpart': 'user_localpart'}

def unstr(s):
    if s is None: return None
    m = re.search(r'Cow::Borrowed\(r?"((?:[^"\\]|\\.)*)"\)', s)
    return m.group(1).replace('\\\\', '\\') if m else None

def field(block, name):
    m = re.search(rf'\b{name}:\s*(Cow::Borrowed\(r?"(?:[^"\\]|\\.)*"\)|[^\n]+)', block)
    return m.group(1) if m else None

def cond_json(inner):
    if inner.startswith('KnownCondition::ContainsDisplayName'):
        return {"kind": "contains_display_name"}
    kind = re.match(r'KnownCondition::(\w+)', inner).group(1)
    key = unstr(field(inner, 'key'))
    if kind == 'EventMatch':
        return {"kind": "event_match", "key": key, "pattern": unstr(field(inner, 'pattern'))}
    if kind == 'EventMatchType':
        return {"kind": "event_match", "key": key,
                "pattern_type": PT[re.search(r'EventMatchPatternType::\w+', field(inner, 'pattern_type')).group(0)]}
    if kind == 'RelatedEventMatchType':
        d = {"kind": "im.nheko.msc3664.related_event_match", "key": key,
             "pattern_type": PT[re.search(r'EventMatchPatternType::\w+', field(inner, 'pattern_type')).group(0)],
             "rel_type": unstr(field(inner, 'rel_type'))}
        inc = field(inner, 'include_fallbacks')
        if inc and 'None' not in inc: d["include_fallbacks"] = 'true' in inc
        return d
    if kind == 'EventPropertyIs':
        v = field(inner, 'value')
        val = True if 'Bool(true)' in v else (False if 'Bool(false)' in v else unstr(v))
        return {"kind": "event_property_is", "key": key, "value": val}
    if kind == 'ExactEventPropertyContainsType':
        return {"kind": "event_property_contains", "key": key,
                "value_type": PT[re.search(r'EventMatchPatternType::\w+', field(inner, 'value_type')).group(0)]}
    if kind == 'RoomMemberCount':
        return {"kind": "room_member_count", "is": unstr(field(inner, 'is'))}
    if kind == 'SenderNotificationPermission':
        return {"kind": "sender_notification_permission", "key": key}
    if kind == 'RoomVersionSupports':
        return {"kind": "org.matrix.msc3931.room_version_supports", "feature": unstr(field(inner, 'feature'))}
    if kind == 'Msc4306ThreadSubscription':
        return {"kind": "io.element.msc4306.thread_subscription", "subscribed": 'true' in field(inner, 'subscribed')}
    raise SystemExit("unhandled condition: " + kind)

def parse_conditions(text):
    out, depth, start = [], 0, None
    for i, ch in enumerate(text):
        if start is None and text.startswith('Condition::Known(', i):
            start, depth = i, 0
        if start is not None:
            if ch == '(': depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    out.append(text[start + len('Condition::Known('):i].strip()); start = None
    return [cond_json(c) for c in out]

def split_rules(body):
    out = []
    for m in re.finditer(r'PushRule \{', body):
        i = m.end() - 1
        depth = 0
        for j in range(i, len(body)):
            if body[j] == '{': depth += 1
            elif body[j] == '}':
                depth -= 1
                if depth == 0:
                    out.append(body[i + 1:j]); break
    return out

out = {}
for name, body in groups_src.items():
    rules = []
    for block in split_rules(body):
        rid = unstr(field(block, 'rule_id'))
        if rid is None: continue
        cm = re.search(r'conditions:\s*Cow::Borrowed\(&\[(.*?)\]\),\s*\n\s*actions:', block, re.S)
        am = re.search(r'actions:\s*Cow::Borrowed\(&\[(.*?)\]\)', block, re.S)
        acts = []
        if am:
            for tok in re.findall(r'Action::Notify|HIGHLIGHT_FALSE_ACTION|HIGHLIGHT_ACTION|SOUND_ACTION|RING_ACTION', am.group(1)):
                acts.append(ACTIONS[tok])
        rules.append({"rule_id": rid,
                      "priority_class": int(re.search(r'priority_class:\s*(\d+)', block).group(1)),
                      "conditions": parse_conditions(cm.group(1)) if cm else [],
                      "actions": acts,
                      "default_enabled": 'default_enabled: false' not in block})
    out[name] = rules
    print(f"{name:34} {len(rules):2}", file=sys.stderr)
json.dump(out, open(sys.argv[1], 'w'), indent=1)
