#!/usr/bin/env python

def main(s):
    upper_sum = 0
    lower_sum = 0
    for c in s:
        if upper_check(c):
            upper_sum = upper_sum + 1
        if lower_check(c):
            lower_sum = lower_sum + 1
    
    print("Lower case characters: " + str(lower_sum))
    print("Upper case characters: " + str(upper_sum))
        
    
def upper_check(c):
    if c >= 'A' and c <= 'Z':
        return True
    else:
        return False
    
def lower_check(c):
    if c >= 'a' and c <= 'z':
        return True
    else:
        return False    
    
if __name__ == "__main__":
    s = input("Input String: ")
    main(s)