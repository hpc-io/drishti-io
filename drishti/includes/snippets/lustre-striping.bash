lfs setstripe -S 4M -c 64 /path/to/your/directory/or/file

# -S defines the stripe size (i.e., the size in which the file will be broken down into)
# -c defines the stripe count (i.e., how many servers will be used to distribute stripes of the file)