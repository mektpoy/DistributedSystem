import os

Input = open("wordcount.txt", "r")
Output = open("freword.txt", "w")
for line in Input:
	now = line.split()
	if int(now[1]) > 100:
		print(now[0], file=Output)