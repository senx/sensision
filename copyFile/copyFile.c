#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char** argv) {


  if (argc != 2) { 
    
    printf("Usage: %s [/proc]/XX/fileToDump\n", argv[0]);
    printf("Note: Only files under /proc can be dumped\n");
    printf("Sample: to dump /proc/net/ip_conntrack, use %s net/ip_conntrack - do not provide /proc at the beginning\n", argv[0]);
    exit(-4);

  } else {
    
    // Elevate our privileges
    setuid(0);

    FILE *fp;

    char *fname = malloc(7 + strlen(argv[1]));
    strcat(fname, "/proc/");
    strcat(fname, argv[1]);

    if (NULL != strstr(fname, "..")) {
       exit(-3);
    }

    fp = fopen(fname, "r");

    char buf[8192];

    if (NULL != fp) {
      int nread = 0;
      while ((nread = fread(buf, 1, sizeof buf, fp)) > 0) {
        fwrite(buf, 1, nread, stdout);
      } 
      if (ferror(fp)) {
         exit(-1);
      }
      fclose(fp);
      exit(0);
    } else {
      exit(-2);
    }
  }
}