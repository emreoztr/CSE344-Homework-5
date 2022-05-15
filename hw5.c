#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <pthread.h>

#define NO_EINTR(stmt) while((stmt) < 0 && errno == EINTR);

int n;
int thread_count;
char *matrix_a;
char *matrix_b;
char *matrix_c;


int check_arguments(int argc, char *argv[], char *matrix_file_a, char *matrix_file_b, char *output_file, int *n, int *thread_count);
void *calculation_thread(void *arg);
int read_matrices(const char* matrix_file_a, const char *matrix_file_b, char *matrix_a, char *matrix_b, int matrix_row_col_size);

void __matrix_multiply(char *matrix_a, char *matrix_b, char *matrix_c, int matrix_row_col_size, int column_num, int thread_count);

typedef struct calc_thread_args {
    int thread_id;
} CalcThreadArgs;

int main(int argc, char *argv[])
{
    char matrix_file_a[100];
    char matrix_file_b[100];
    char output_file[100];
    int n;
    int thread_count;


    int fd;

    int matrix_row_col_size;
    int read_bytes;
    pthread_t *threads;
    CalcThreadArgs *thread_args;

    if(check_arguments(argc, argv, matrix_file_a, matrix_file_b, output_file, &n, &thread_count) < 0){
        fprintf(stderr, "Error in arguments, usage: ./hw5 -i filePath1 -j filePath2 -o output -n 4 -m 2\n");
        return -1;
    }

    matrix_row_col_size = (int) pow(2.0, (double) n);

    matrix_a = (char *)malloc(matrix_row_col_size * sizeof(char));
    matrix_b = (char *)malloc(matrix_row_col_size * sizeof(char));
    matrix_c = (char *)malloc(matrix_row_col_size * sizeof(char));

    if(matrix_a == NULL || matrix_b == NULL || matrix_c == NULL){
        perror("matrix memory malloc: ");
        return -1;
    }

    if(read_matrices(matrix_file_a, matrix_file_b, matrix_a, matrix_b, matrix_row_col_size) < 0){
        fprintf(stderr, "Error in reading matrices\n");
        return -1;
    }

    threads = (pthread_t *)malloc(thread_count * sizeof(pthread_t));
    if(threads == NULL){
        perror("thread memory malloc: ");
        return -1;
    }

    thread_args = (CalcThreadArgs *)malloc(thread_count * sizeof(CalcThreadArgs));

    for(int i = 0; i < thread_count; i++){
        thread_args[i].thread_id = i;
        if(pthread_create(&threads[i], NULL, calculation_thread, (void *) (&thread_args[i])) != 0){
            perror("pthread_create: ");
            return -1;
        }
    }

    for(int i = 0; i < thread_count; i++){
        if(pthread_join(threads[i], NULL) != 0){
            perror("pthread_join: ");
            return -1;
        }
    }

    fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(fd < 0){
        perror("open: ");
        return -1;
    }

    //TODO
    write(fd, matrix_c, matrix_row_col_size);

    return 0;
}

int check_arguments(int argc, char *argv[], char *matrix_file_a, char *matrix_file_b, char *output_file, int *n, int *thread_count){
    int flag_file_a = 0;
    int flag_file_b = 0;
    int flag_output = 0;
    int flag_n = 0;
    int flag_thread_count = 0;

    if (argc != 11){
        return -1;
    }

    for(int i = 1; i < argc - 1; ++i){
        if(strcmp(argv[i], "-i") == 0){
            flag_file_a = 1;
            strcpy(matrix_file_a, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-j") == 0){
            flag_file_b = 1;
            strcpy(matrix_file_b, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-o") == 0){
            flag_output = 1;
            strcpy(output_file, argv[i + 1]);
        }
        else if(strcmp(argv[i], "-n") == 0){
            flag_n = 1;
            *n = atoi(argv[i + 1]);
        }
        else if(strcmp(argv[i], "-m") == 0){
            flag_thread_count = 1;
            *thread_count = atoi(argv[i + 1]);
        }
        else{
            return -1;
        }
    }

    if(flag_file_a == 0 || flag_file_b == 0 || flag_output == 0 || flag_n == 0 || flag_thread_count == 0){
        return -1;
    }

    return 0;
}


int read_matrices(const char* matrix_file_a, const char *matrix_file_b, char *matrix_a, char *matrix_b, int matrix_row_col_size){
    int fd; 
    int read_bytes;

    fd = open(matrix_file_a, O_RDONLY);
    if(fd < 0){
        perror("open matrix_file_a: ");
        return -1;
    }

    NO_EINTR(read_bytes = read(fd, matrix_a, matrix_row_col_size * sizeof(char)));
    if(read_bytes < 0){
        perror("read matrix_file_a: ");
        return -1;
    }
    else if(read_bytes < matrix_row_col_size * sizeof(char)){
        fprintf(stderr, "read matrix_file_a: insufficient characters in file, read only %d bytes\n", read_bytes);
        return -1;
    }

    if(close(fd)){
        perror("close matrix_file_a: ");
        return -1;
    }

    fd = open(matrix_file_b, O_RDONLY);
    if(fd < 0){
        perror("open matrix_file_b: ");
        return -1;
    }

    NO_EINTR(read_bytes = read(fd, matrix_b, matrix_row_col_size * sizeof(char)));
    if(read_bytes < 0){
        perror("read matrix_file_b: ");
        return -1;
    }
    else if(read_bytes < matrix_row_col_size * sizeof(char)){
        fprintf(stderr, "read matrix_file_b: insufficient characters in file, read only %d bytes\n", read_bytes);
        return -1;
    }

    if(close(fd)){
        perror("close matrix_file_b: ");
        return -1;
    }

    return 0;
}

void __matrix_multiply(char *matrix_a, char *matrix_b, char *matrix_c, int matrix_row_col_size, int column_num, int thread_count){
    for(int i = column_num; i < matrix_row_col_size; i+=thread_count){
        for(int j = 0; j < matrix_row_col_size; j++){
            for(int k = 0; k < matrix_row_col_size; k++){
                matrix_c[i * matrix_row_col_size + j] += matrix_a[i * matrix_row_col_size + k] * matrix_b[k * matrix_row_col_size + j];

            }
        }
    }
}