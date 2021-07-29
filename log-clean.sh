echo "Getting Configurations..."
BASE_LOG_FOLDER="{{params.directory}}"
WORKER_SLEEP_TIME="{{params.sleep_time}}"
LOG_CLEANUP_PROCESS_LOCK_FILE="/tmp/airflow_log_cleanup_worker.lock"
sleep ${WORKER_SLEEP_TIME}s

MAX_LOG_AGE_IN_DAYS="{{params.max_log_days}}"

ENABLE_DELETE="{{params.enable_delete}}"
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=`eval $1`
    echo "Process will be Deleting the following File(s)/Directory(s):"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | \
    grep -v '^$' | wc -l` File(s)/Directory(s)"     \
    # "grep -v '^$'" - removes empty lines.
    # "wc -l" - Counts the number of lines
    echo ""
    if [ "${ENABLE_DELETE}" == "True" ];
    then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
        then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code \
                    '${DELETE_STMT_EXIT_CODE}'"

                echo "Removing lock file..."
                rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
                if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
                    echo "Error removing the lock file. \
                    Check file permissions.\
                    To re-run the DAG, ensure that the lock file has been \
                    deleted ()."
                    exit ${REMOVE_LOCK_FILE_EXIT_CODE}
                fi
                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No File(s)/Directory(s) to Delete"
        fi
    else
        echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
    fi
}


if [ ! -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """ ]; then

    echo "Lock file not found on this node! \
    Creating it to prevent collisions..."
    touch """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    CREATE_LOCK_FILE_EXIT_CODE=$?
    if [ "${CREATE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error creating the lock file. \
        Check if the airflow user can create files under tmp directory. \
        Exiting..."
        exit ${CREATE_LOCK_FILE_EXIT_CODE}
    fi

    echo ""
    echo "Running Cleanup Process..."

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime \
     +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    echo "Finished Running Cleanup Process"

    echo "Deleting lock file..."
    rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    REMOVE_LOCK_FILE_EXIT_CODE=$?
    if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted ()."
        exit ${REMOVE_LOCK_FILE_EXIT_CODE}
    fi

else
    echo "Another task is already deleting logs on this worker node. \
    Skipping it!"
    echo "If you believe you're receiving this message in error, kindly check \
    if exists and delete it."
    exit 0
fi
