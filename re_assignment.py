import re 
 
pattern= r"We.*Data$" 

txt = "We love programming with Big Data" 

x = re.search(pattern,txt)
if x:
    print("found: " + x.group() )
else:
    print("not found")   