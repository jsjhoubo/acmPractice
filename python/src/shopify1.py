import csv
import random

people =[]
family =[]

with open('file.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        people.append(row['name'])
        family.append(row['family']) 

graph={}

n =len(people)

for i in range(n):
    p =people[i]
    graph[p]=[]
    for j in range(n):
        if i ==j:
            continue
        q =people[j]
        if family[i] == family[j]:
            continue
        graph[p].append(q)


rd =random.randint(0,n-1)

mat={}
s = people[rd]
size =0
while True:
    
    if len(mat) ==size:
        break

