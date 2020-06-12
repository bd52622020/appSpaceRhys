#!/usr/bin/env python

def main():
    for i in range(1,10,1):
        if i < 6:
            for j in range(1,6,1):
                if i >= j:
                    print("*", end=" ")
        else:
            for j in range(0,4,1):
                if i-6 <= j:
                    print("*", end=" ")
        print("")
    
    
if __name__ == "__main__":
    main()