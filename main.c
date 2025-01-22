/* https://cstack.github.io/db_tutorial/parts/part1.html --项目地址*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>

#define COLUMN_USERNAME_SIZE 32 // 用户名字段长度
#define COLUMN_EMAIL_SIZE 255   // 邮箱字段长度
#define TABLE_MAX_PAGES 100  // 最大页数

/**
 * sizeof_of_attribute: 计算结构体中某个成员的大小
 */
#define sizeof_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)

/**
 * Row 结构
 * id: 主键
 * username: 用户名
 * email: 邮箱
 */
typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
}Row;

/**
 * NodeType 节点类型
 * NODE_INTERNAL: 内部节点
 * NODE_LEAF: 叶子节点
 */
typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

/**
 * 公共头结点布局
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * 叶子头节点布局
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


const uint32_t ID_SIZE = sizeof_of_attribute(Row, id);              // ID大小
const uint32_t USERNAME_SIZE = sizeof_of_attribute(Row, username);  // 用户名大小
const uint32_t EMAIL_SIZE = sizeof_of_attribute(Row, email);        // 邮箱大小
const uint32_t ID_OFFSET = 0;                                       // ID偏移
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;               // 用户名偏移
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;      // 邮箱偏移
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;     // 行大小
const uint32_t PAGE_SIZE = 4096;                                    // 4KB 一页
// const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;                // 每页多少行
// const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;    // 最大行数

/**
 * 叶子节点体布局
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);                                           // 键大小   
const uint32_t LEAF_NODE_KEY_OFFSET = 0;                                                        // 键大小   
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;                                                 // 值大小                 
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;              // 值偏移
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;                 // 单元格大小
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;                   // 叶子节点剩余空间
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;           // 叶子节点最大单元数

// 左右分裂点
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =  (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/**
 * Pager 结构
 * file_descriptor: 文件描述符
 * file_length: 文件长度
 * pages: 页数组
 */
typedef struct{
    int file_descriptor;    
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
}Pager;


/**
 * InputBuffer 结构
 * buffer: 输入缓冲区
 * buffer_length: 缓冲区长度
 * input_length: 输入长度
 */
typedef struct{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;

/**
 * new_input_buffer: 创建一个新的输入缓冲区
 * 返回值: 输入缓冲区指针
 */
InputBuffer* new_input_buffer(){
    InputBuffer *input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

/**
 * MetaCommandResult 命令类型
 * META_COMMAND_SUCCESS: 命令成功
 * META_COMMAND_UNRECOGNIZED_COMMAND: 未识别的命令
 */
typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;


/**
 * PrepareResult 准备结果类型
 * PREPARE_SUCCESS: 准备成功
 * PREPARE_UNRECOGNIZED_STATEMENT: 未识别的语句
 * PREPARE_SYNTAX_ERROR: 语法错误
 */
typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
}PrepareResult;

/**
 * StatementType 语句类型
 * STATEMENT_INSERT: 插入语句
 * STATEMENT_SELECT: 查询语句
 */
typedef enum { 
    STATEMENT_INSERT,
    STATEMENT_SELECT 
}StatementType;

/**
 * Statement 语句结构
 * type: 语句类型
 * row_to_insert: 要插入的行
 */
typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

/**
 * Table 结构
 * num_rows: 行数
 * page: 页数组
 * 每个页的大小为 4KB，一页可以存放 100 行数据
 */
typedef struct {
    // uint32_t num_rows;  // 行数
    Pager* pager;       // 分页器
    uint32_t root_page_num; // 根页号
}Table;

typedef enum { 
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL, 
    EXECUTE_UNRECOGNIZED_STATEMENT,
    EXECUTE_DUPLICATE_KEY
}ExecuteResult;

enum ExecuteResult_t{
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
};

/**
 * 游标结构
 * table: 表指针
 * row_num: 行号
 * end_of_table: 是否到达表尾
 */
typedef struct{
    Table* table;
    // uint32_t row_num;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
}Cursor;



// 获取单元格个数nmu_cells的偏移地址
uint32_t* leaf_node_num_cells(void* node){
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// 获取node单元格的偏移地址
void* leaf_node_cell(void* node,uint32_t cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

// 获取node单元格的key指针偏移
u_int32_t* leaf_node_key(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num);
}

// 获取node单元格的value指针偏移
void* leaf_node_value(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num) + LEAF_NODE_KEY_SIZE;
}

// 初始化node为叶子节点
void initialize_leaf_node(void* node){ 
    set_node_type(node, NODE_LEAF);
    *leaf_node_num_cells(node) = 0;
 }

/**
 * serialize_row: 将 source 结构序列化到 destination 指针指向的内存中
 */
void serialize_row(Row* source, void* destination){
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

/**
 * get_page: 获取指定页的页面指针，pager->pages[page_num]
 * pager: 分页器指针
 * page_num: 页号
 * 返回值: 页面指针
 */
void* get_page(Pager* pager, uint32_t page_num){
    if(page_num >= TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    // 检查该页是否已经加载到内存中
    if(pager->pages[page_num] == NULL){
        // 如果没有，则为该页分配内存
        void* page = malloc(PAGE_SIZE);
        
        // 计算文件中已经存在的页数
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // 如果文件长度不是 PAGE_SIZE 的整数倍，则还需要额外一页 
        if(pager->file_length % PAGE_SIZE != 0){
            num_pages++;
        }   

        if(page_num <= num_pages){
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1){
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        
        if(page_num >= pager->num_pages){
            pager->num_pages = page_num + 1;
        }

        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

/**
 * deserialize_row: 将 Row 结构从 source 指针指向的内存中反序列化到 destination 指针指向的结构中
 */
void deserialize_row(void* source, Row* destination){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(destination->username, source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(destination->email, source + EMAIL_OFFSET, EMAIL_SIZE);
}

/**
 * free_input_buffer: 释放输入缓冲区
 */
void free_input_buffer(InputBuffer *input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

/**
 * 刷新缓冲区，将pager的pages[page_num]缓冲区的内容写入文件，大小为size
 */
void pager_flush(Pager* pager, uint32_t page_num){
    if(pager->pages[page_num] == NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if(offset == -1){
        printf("Error seeking file: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if(bytes_written == -1){
        printf("Error writing file: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

/**
 * db_close: 关闭数据库
 */
void db_close(Table* table){
    Pager* pager = table->pager;

    // 计算表中有多少页是满页
    // - uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    // 遍历每个满页，确保其从内存刷入磁盘，并释放内存  
    for (uint32_t  i = 0; i < pager->num_pages; i++)
    {
        if(pager->pages[i] == NULL){
            continue;
        } 
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // 处理表中剩余的不足一页的行
    // uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;      
    // if(num_additional_rows > 0){
    //     uint32_t page_num = num_full_pages;
    //     if(pager->pages[page_num] != NULL){
    //         pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
    //         free(pager->pages[page_num]);
    //         pager->pages[page_num] = NULL;
    //     }
    // }
    int result = close(pager->file_descriptor);
    if(result == -1){
        printf("Error closing file: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void* page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}


/**
 * 获取表开始游标
 * 返回值: 表开始游标指针
 */
Cursor* table_start(Table* table){
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    // cursor->row_num = 0;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

/**
 * 获取表结束游标
 * 返回值: 表结束游标指针
 */
// Cursor* table_end(Table* table){
//     Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
//     cursor->table = table;
//     cursor->page_num = table->root_page_num;
//     void* root_node = get_page(table->pager,table->root_page_num);
//     uint32_t num_cells = *leaf_node_num_cells(root_node);
//     cursor->cell_num = num_cells;
//     cursor->end_of_table = true;
//     return cursor;
// }

void print_leaf_node(void* node){
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("Leaf node with %d cells:\n", num_cells);
    for(uint32_t i = 0; i < num_cells; i++){
        uint32_t key = *leaf_node_key(node, i);
        printf("    - %d : %d\n",i,key);
    }
}

void print_constants(){
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_KEY_SIZE: %d\n", LEAF_NODE_KEY_SIZE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}


/**
 * do_meta_command: 执行元命令
 * 返回值: 命令执行结果
 */
MetaCommandResult do_meta_command(InputBuffer* input_buffer,Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(EXIT_SUCCESS);
  }
  else if(strcmp(input_buffer->buffer, ".btree") == 0){
    printf("Tree:\n");
    print_leaf_node(get_page(table->pager, 0));
    return META_COMMAND_SUCCESS;
  }
  else if(strcmp(input_buffer->buffer, ".constants") == 0){
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  }
  else{
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

/**
 *  prepare_insert: 准备插入语句
 */
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_str = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if(id_str == NULL || username == NULL || email == NULL){
        return PREPARE_STRING_TOO_LONG;
    }

    int id = atoi(id_str);
    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    if(strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

/**
 * prepare_statement: 准备语句
 * 返回值: 准备结果
 */
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if(strncmp(input_buffer->buffer,"insert ",6) == 0){
        return prepare_insert(input_buffer, statement);
    }

    if(strncmp(input_buffer->buffer,"select ",6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * cursor_value: 通过游标获取指定行的槽位指针
 * 返回值: 行槽位指针
 * 说明: 该函数根据行号计算出所在页的指针，并计算出该行在页中的偏移，然后返回指向该行的指针
 */
void *cursor_value(Cursor* cursor){
    // uint32_t row_num = cursor->row_num;
    // uint32_t page_num = row_num / ROWS_PER_PAGE;                // 所在页号 
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);      // 所在页指针
    return leaf_node_value(page, cursor->cell_num);  
}

/**
 * 游标前移
 * 说明: 该函数将游标指向下一行，并判断是否到达表尾
 */
void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;
    if(cursor->cell_num == *leaf_node_num_cells(node)){
        cursor->end_of_table = true;
    }
}

  /*
    Until we start recycling free pages, new pages will always
    go onto the end of the database file
  */
uint32_t get_unused_page_num(Pager* pager){
    return pager->num_pages;
}

/**
 * print_row: 打印 Row 结构
 */
void print_row(Row* row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value){
    /*  Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent.
    */
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);    

    /*
        All existing keys plus new key should be divided
        evenly between old (left) and new (right) nodes.
        Starting from the right, move each key to correct position.
    */
    for(int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--){
        void* destination_node;
        if(i >= LEAF_NODE_LEFT_SPLIT_COUNT){
            destination_node = new_node;
        }
        else{
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);

        if(i == cursor->cell_num){
            serialize_row(value, destination);
        }
        else if(i > cursor->cell_num){
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        }
        else{
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    if(is_node_root(old_node)){
        return create_new_root(cursor->table, new_page_num);
    }
    else{
        printf("Need to implement updating parent node.\n");
        exit(EXIT_FAILURE);
    }
}


void leaf_node_insert(Cursor* cursor,uint32_t key,Row* value){
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        // printf("Need to implement splitting leaf nodes.\n");
        // exit(EXIT_FAILURE);
        leaf_node_split_and_insert(cursor,key,value);
        return;
    }
    if(cursor->cell_num < num_cells){
        for (uint32_t i = num_cells; i > cursor->cell_num; i--)
        {
            memcpy(leaf_node_cell(node,i),leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);
        }
    }
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node,cursor->cell_num)) = key;
    serialize_row(value,leaf_node_value(node,cursor->cell_num));
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key){
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // 二分查找
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node,index);
        if(key_at_index == key){
            cursor->cell_num = index;
            return cursor;
        }
        if(key < key_at_index){
            one_past_max_index = index;
        }
        else{
            min_index = index + 1;
        }
    }
    cursor->cell_num = min_index;
    return cursor;
}

NodeType get_node_type(void* node){
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type){
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

Cursor* table_find(Table* table, uint32_t key){
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);
    if(get_node_type(root_node) == NODE_LEAF){
        return leaf_node_find(table, root_page_num, key);
    }
    else{
        printf("Need to implement searching an internal node.\n");
        exit(EXIT_FAILURE);
    }
}


/**
 * execute_insert: 执行插入语句
 * 返回值: 执行结果
 */
ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));
    // if(num_cells >= LEAF_NODE_MAX_CELLS){
    //     return EXECUTE_TABLE_FULL;
    // }

    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);
    if(cursor->cell_num < num_cells){
        uint32_t key_at_index = *leaf_node_key(node,cursor->cell_num);
        if(key_at_index == key_to_insert){
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    // Cursor* cursor = table_end(table);
    // serialize_row(row_to_insert, row_slot(table, table->num_rows));
    // serialize_row(row_to_insert, cursor_value(cursor));
    // table->num_rows++;
    leaf_node_insert(cursor,row_to_insert->id,row_to_insert);
    free(cursor);
    return EXECUTE_SUCCESS;
}

/**
 * execute_select: 执行查询语句
 * 返回值: 执行结果
 * 说明: 该函数遍历表中的所有行，并打印每行的内容
 */
ExecuteResult execute_select(Statement* statement, Table* table){
    Cursor* cursor = table_start(table);
    Row row;
    while (!(cursor->end_of_table))
    {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);

    return EXECUTE_SUCCESS;
}

/**
 * execute_statement: 执行语句
 * 返回值: 执行结果
 */
ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement->type){
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
        default:
            return EXECUTE_UNRECOGNIZED_STATEMENT;
    }
}

/**
 * print_prompt: 打印提示符
 */
void print_prompt(){
    printf("db > ");
}


/**
 * read_input: 读取命令行输入
 */
void read_input(InputBuffer *input_buffer){
    ssize_t bytes_read = getline(&(input_buffer->buffer),&(input_buffer->buffer_length),stdin);
    if(bytes_read <= 0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    //忽略结尾的换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

Pager* pager_open(const char* filename){
    int fd = open(filename, O_RDWR | O_EXCL | S_IWUSR | S_IRUSR);
    if(fd == -1){
        printf("Error opening file %s: %s\n", filename, strerror(errno));
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    
    Pager* pager = (Pager*)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;
    if(file_length % PAGE_SIZE != 0){
        printf("Db file is not a whole number of pages. Corrupt file?\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
}

/**
 * new_table: 创建一个新的 Table 结构
 * 返回值: Table 结构指针
 */
Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);
    // uint32_t num_rows = pager->file_length / ROW_SIZE;
    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    if(pager->num_pages == 0){
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}

int main(int argc,char **argv){
    if(argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();
    while (true)
    {
        print_prompt();
        read_input(input_buffer);
        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer,table))
            {
            case META_COMMAND_SUCCESS:
                continue;
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized meta command '%s'\n", input_buffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        
        case(PREPARE_STRING_TOO_LONG):
            printf("String too long.\n");
            continue;
        
        case(PREPARE_NEGATIVE_ID):
            printf("ID must be non-negative.\n");
            continue;

        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement.\n");
            continue;
        
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Unrecognized statement '%s'\n", input_buffer->buffer);
            continue;
        }

        switch (execute_statement(&statement,table))
        {
        case EXECUTE_SUCCESS:
            printf("Executed.\n");
            break;
        case EXECUTE_DUPLICATE_KEY:
            printf("Error: Duplicate key.\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Error: Table full.\n");
            break;
        // case EXECUTE_UNRECOGNIZED_STATEMENT:
        //     printf("Error: Unrecognized statement.\n");
        //     break;
        // }        
    }
    return 0;
}