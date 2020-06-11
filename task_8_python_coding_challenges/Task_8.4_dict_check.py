#!/usr/bin/env python

def main(d,query):
    for k, v in d.items():
        if k == query:
            print("Value of key: " + str(v))  
            return True
    print("Key not found.")
    return False
    
if __name__ == "__main__":
    d = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60} 
    query = 5
    main(d,query)