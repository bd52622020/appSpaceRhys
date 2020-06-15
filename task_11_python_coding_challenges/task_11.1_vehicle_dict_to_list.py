#!/usr/bin/env python

def main(dict1):
    result = {k.upper() for (k,v) in dict1.items() if v < 5000}
    print(result)
    return result
    
if __name__ == "__main__":
    dict1 ={"Sedan": 1500, "SUV": 2000, "Pickup": 2500, "Minivan": 1600, "Van": 2400, "Semi": 13600, "Bicycle": 7, "Motorcycle": 110} 
    main(dict1)