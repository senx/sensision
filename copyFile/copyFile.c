#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char** argv) {
  
  //
  // Exclusions: dump of these files (/proc/XXX/mem, ...) is forbidden
  //

  const char *exclusions[] = {"mem","fd","kcore"};

  if (argc != 2) {
    
    printf("Usage: %s [/proc]/XX/fileToDump\n", argv[0]);
    printf("Note: Only files under /proc can be dumped\n");
    printf("Sample: to dump /proc/net/ip_conntrack, use %s net/ip_conntrack - do not provide /proc at the beginning\n", argv[0]);
    exit(-5);

  } else {

    char *fname = malloc(7 + strlen(argv[1]));
    strcat(fname, "/proc/");
    strcat(fname, argv[1]);

    if (NULL != strstr(fname, "..")) {
      printf(".. is forbidden (%s)\n", argv[1]);
      exit(-5);
    }

    for(int i = 0; i < sizeof(exclusions) / sizeof(exclusions[0]); i++) {
      // Path ends with by the current exclusion (/proc/{pid}/mem)
      int diff = strlen(fname)-strlen(exclusions[i]);
      if ((diff >= 0) && (0 == strcmp(&fname[diff], exclusions[i]))) {
        printf("Dump of /proc/%s is forbidden\n", argv[1]);
        exit(-4);
      }

      // The current exclusion is a directory in the path (/proc/{pid}/fd/0)
      char *exclusionPattern = malloc(2 + strlen(exclusions[i]));
      strcat(exclusionPattern, "/");
      strcat(exclusionPattern, exclusions[i]);
      strcat(exclusionPattern, "/");
      if (NULL != strstr(fname, exclusionPattern)) {
        printf("Dump of /proc/%s is forbidden\n", argv[1]);
        exit(-3);
      }
    }
    
    // Elevate our privileges
    setuid(0);

    FILE *fp;
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