#!/usr/bin/env python
import platform, socket

def main():
    print(platform.uname())
    print("Hostname: " + socket.gethostname())
    
if __name__ == "__main__":
    main()