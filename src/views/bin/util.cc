/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2014 Couchbase, Inc.
 *
 * @author Sarath Lakshman  <sarath@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "config.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "../mapreduce/mapreduce.h"

static void do_exit(int ret, int uses_v8)
{
    /* Notify and delete conditional variables,
    and join waiting threads when v8 is used */
    if(uses_v8) {
        deinit_terminator_thread(true /*fatal_exit*/);
    }
    _exit(ret);
}

static void exit_thread_helper(void *args)
{
    char buf[4];
    int len = fread(buf, 1, 4, stdin);

    int uses_v8 = *reinterpret_cast<int*>(args);

    /* If the other end closed the pipe */
    if (len == 0) {
        do_exit(1, uses_v8);
    } else if (len == 4 && !strncmp(buf, "exit", 4)) {
        do_exit(1, uses_v8);
    } else {
        fprintf(stderr, "Error occured waiting for exit message (%d)\n", len);
        do_exit(2, uses_v8);
    }
}

/* Start a watcher thread to gracefully die on exit message */
int start_exit_listener(cb_thread_t *id, int uses_v8)
{
    void *args = reinterpret_cast<void*>(&uses_v8);
    int ret = cb_create_thread(id, exit_thread_helper, args, 1);
    if (ret < 0) {
        /* For differentiating from couchstore_error_t */
        return -ret;
    }

    return ret;
}

int set_binary_mode()
{
    if (platform_set_binary_mode(stdin) < 0 ||
            platform_set_binary_mode(stdout) < 0 ||
            platform_set_binary_mode(stderr) < 0) {
        return -1;
    }
    return 0;
}
