#!usr/bin/env python
import csv

with open("./Shakespeare.txt","rt") as file:
	lines = 0
	words = 0
	chars = 0
	vowels = 0
	nums = 0
	num_flag = 0

	for line in file:
		word_list = line.split()
		lines = lines + 1
		words = words + len(word_list)
		for word in word_list: 
			for char in word:
				chars = chars + 1
				if(char=='A' or char=='a' or char=='E' or char =='e' or char=='I' or char=='i' or char=='O' or char=='o' or char=='U' or char=='u'):
					vowels = vowels + 1
				if char.isdigit():
					num_flag = 1
			if num_flag == 1:
				nums = nums + 1
			num_flag = 0

	with open('result.csv', mode='w') as output:
		output_writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		output_writer.writerow(['Lines', lines])
		output_writer.writerow(['Words', words])
		output_writer.writerow(['Characters', chars])
		output_writer.writerow(['Vowels', vowels])
		output_writer.writerow(['Numbers', nums])

