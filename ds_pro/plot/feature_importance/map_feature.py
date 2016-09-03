import csv
dict_list = []
with open('feature_importance.csv', 'rb') as f:
	reader = csv.reader(f)
	for row in reader:
		if not row[0].isdigit():
			dict_list.append({'key':row[0], 'value':int(row[1])})
print dict_list
