#!/usr/bin/env python
from re import findall

def main(str1):
    pattern = "^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[$#@]).{6,16}$"
    if findall(pattern, str1):
        print("Valid.")
    else:
        print("Invalid.")
    
if __name__ == "__main__":
    u_input = input("Please enter a password: ")
    main(u_input)