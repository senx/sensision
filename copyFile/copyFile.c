#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
  char buf[BUFSIZ];
  
  if (argc != 3) { 
    printf("Usage: %s /path/to/fileSource /path/to/fileTarget\n", argv[0]);
    printf("Note: Create fileTarget before with the right uid/gid.\n");
    printf("Use - as fileTarget to display file content to stdout.\n");
    return 1;
  } else {
    if (strcmp(argv[2],"-") == 0){
      setuid(0);
      sprintf(buf, "/bin/cat %s", argv[1], argv[2]);
      system(buf);
    } else {
      setuid(0);
      sprintf(buf, "/bin/cat %s > %s", argv[1], argv[2], argv[2]);
      system(buf);
    }
  } 
   return 0;
}

