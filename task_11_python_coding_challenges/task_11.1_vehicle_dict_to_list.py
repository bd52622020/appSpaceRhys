#!/usr/bin/env python

def main(dict1):
    dict2 = {k:v for (k,v) in dict1.items() if v < 5000}
    result = list(map(lambda x: x.upper(), list(dict2.keys())))
    print(result)
    return result
    
if __name__ == "__main__":
    dict1 ={"Sedan": 1500, "SUV": 2000, "Pickup": 2500, "Minivan": 1600, "Van": 2400, "Semi": 13600, "Bicycle": 7, "Motorcycle": 110} 
    main(dict1)