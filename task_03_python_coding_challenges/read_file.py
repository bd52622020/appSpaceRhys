#!/usr/bin/env python


def main(file='test.txt'):
    try:
        rfile=open(file,'r')
        for i in range(0,10,1):
            print(rfile.readline())
        rfile.close()
    except:
        print("File not found.")
        
    
if __name__ == "__main__":
    file = input("Please enter filename: ")
    main(file)