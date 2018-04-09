import json

model = json.loads(open('a.json').read())

locks = 0
for b in model['backends']:
	if b['wait_event_type'] == 'Lock':
		locks += 1

print('There are %d backends waiting for locks.' % locks)

