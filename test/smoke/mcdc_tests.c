#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "mcdc_utils.h"

/* --------------------------
   TEST MAIN()
   -------------------------- */

int test_extract_dir_name(void) {
    /* 1. Create a temporary file */
    char tmpl[] = "/tmp/mcdc_manifest_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) {
        perror("mkstemp");
        return 1;
    }

    printf("Created temp file: %s\n", tmpl);

    /* Sample manifest content */
    const char *manifest_text =
        "# MC/DC manifest example\n"
        "id=42\n"
        "created=2025-01-01T00:00:00Z\n"
        "dict_file=/var/lib/mcdc/dict_42.bin\n"
        "namespaces=default:foo\n"
        "level=1\n"
        "signature=abcdef\n";

    /* Write manifest */
    if (write(fd, manifest_text, strlen(manifest_text)) < 0) {
        perror("write");
        close(fd);
        return 1;
    }
    close(fd);

    /* 2. Read it back into memory */
    FILE *fp = fopen(tmpl, "rb");
    if (!fp) {
        perror("fopen");
        return 1;
    }

    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *buf = malloc(sz);
    if (!buf) {
        perror("malloc");
        fclose(fp);
        return 1;
    }

    if (fread(buf, 1, sz, fp) != (size_t)sz) {
        perror("fread");
        free(buf);
        fclose(fp);
        return 1;
    }
    fclose(fp);

    printf("Read manifest (%ld bytes):\n%s\n", sz, buf);

    /* 3. Call the extractor */
    char dict_name[256];
    size_t outsize = sizeof(dict_name);
    int rc = mcdc_extract_dict_full_file_name(buf, (size_t)sz, dict_name, &outsize);

    /* 4. Verify */
    if (rc == 0) {
        printf("SUCCESS: Extracted dictionary filename: \"%s\" (len=%zu)\n",
               dict_name, outsize);
    } else {
        printf("ERROR: mcdc_extract_dict_file_name failed, rc=%d (%s)\n",
               rc, strerror(-rc));
    }
    
    char name[64];
    outsize = sizeof(name);
    rc = mcdc_filename_no_ext(dict_name, name, outsize);
    if(rc == 0) {
      printf("SUCCESS: Extracted name: \"%s\"\n", name);
    } else {
      printf("ERROR: mcdc_filename_noext failed, rc=%d\n", rc);
    }

    free(buf);
    unlink(tmpl); /* cleanup */

    return (rc == 0) ? 0 : 1;
}

int main(void) {
  test_extract_dir_name();	
}
