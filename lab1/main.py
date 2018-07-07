import os
import re

Input = open("InvertedIndex.txt", "r")

dataPath = ".\\shakespeare"
Dict = {}

for root, Dir, files in os.walk(dataPath) :
	for filename in files:
		now = open(dataPath + "\\" + filename, "r")
		Dict[filename] = {}
		cnt = 0
		for line in now:
			Dict[filename][cnt] = line.strip()
			cnt += len(line)

InvertedIndex = {}

for line in Input:
	now = line.strip("\n").split("\t")
	InvertedIndex[now[0]] = now[1].split(", ")

while 1:
	print("Please input your query string.")
	query = input()
	check = 0
	if InvertedIndex.get(query):
		for kv in InvertedIndex[query]:
			check = 1
			kv = kv.split(":")
			line = Dict[kv[0]][int(kv[1])]
			print("in", kv[1], "of", kv[0], ":\n   ", re.sub(query, "*"+query+"*", line, flags=re.I))
	if check == 0:
			print("No such word or too normal.")	