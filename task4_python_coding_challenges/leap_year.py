#!/usr/bin/env python

def main(year):
    
    if not year % 4:
        if not year % 100:
            if not year % 400:
                print("Is a leap year.")
            else:
                print("Not a leap year.")
        else:
            print("Is a leap year.")
    else:
        print("Not a leap year.")
        
        
if __name__ == "__main__":
    year = input("Please enter a Year: ")    
    main(int(year))