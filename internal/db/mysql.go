package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func NewMySQL(dsn string)(*sql.DB,error){
	db,err := sql.Open("mysql",dsn)
	if err != nil{
		return nil,fmt.Errorf("open mysql failed: %w",err)
	}

	if err:= db.Ping(); err!= nil{
		return nil,fmt.Errorf("ping mysql failed: %w",err)
	}
	return db,nil

}