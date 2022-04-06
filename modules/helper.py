import os

def find_files(filename, search_path):
   for root, dir, files in os.walk(search_path):
      if filename in files:
         path=os.path.join(root, filename)
         path=path.replace('\\', '/')
         return path
   return ""