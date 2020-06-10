#!usr/bin/env python

original_string = input("input a string: ")
reversed_string = original_string[::-1]
print("original string: " + original_string)
print("reversed string: " + reversed_string)
file = open("output.txt","w")
file.write("original string: " + original_string + "\n")
file.write("reversed string: " + reversed_string)
file.close
