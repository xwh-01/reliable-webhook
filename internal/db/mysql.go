// 数据库连接层，仅负责创建 MySQL 连接并验证可用
// 返回的 *sql.DB 是整个进程唯一的连接池，eventRepo 和 deliveryRepo 共享
package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // 仅注册驱动，不直接引用符号
)

// NewMySQL 创建 MySQL 连接池并执行 ping 验证
// dsn 由 config 包拼装完成，如 "user:password@tcp(127.0.0.1:3306)/webhook_platform?parseTime=true"
func NewMySQL(dsn string)(*sql.DB,error){
	db,err := sql.Open("mysql",dsn)
	if err != nil{
		return nil,fmt.Errorf("open mysql failed: %w",err)
	}

	// Ping 确保连接真正可用，而不仅仅是驱动加载成功
	if err:= db.Ping(); err!= nil{
		return nil,fmt.Errorf("ping mysql failed: %w",err)
	}
	return db,nil

}
