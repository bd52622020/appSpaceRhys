import re 
 
pattern= r"We.*Data" 

txt = "We love programming with Big Data" 

x = re.search(pattern,txt).group() 
print("found: " + x)