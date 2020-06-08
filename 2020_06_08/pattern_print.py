#!/usr/bin/env python

def main():
    for i in range(1,6,1):
        for j in range(0,5,1):
            if i >j:
                print("*", end=" ")
        print("")                
    for i in range(0,4,1):
        for j in range(0,4,1):
            if i <= j:
                print("*", end=" ")
        print("")
    
    
if __name__ == "__main__":
    main()