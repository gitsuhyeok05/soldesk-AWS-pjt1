#!/bin/bash

# MySQL 변수 정의
DB_NAME="damadb"
DB_USER="damauser"
USER_PWD="password"

# SQL 명령어 정의
SQL_COMMANDS=$(cat <<EOF
CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE USER '${DB_USER}'@'%' IDENTIFIED BY '${USER_PWD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'%';
FLUSH PRIVILEGES;

USE ${DB_NAME};

CREATE TABLE gamedatas (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  gold INT NOT NULL DEFAULT 10000,
  last_action VARCHAR(255) DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

EOF
)

# MySQL 접속 및 실행
mysql -u damauser -p -h pjt-seoul-rds.cnwgggeo65yy.ap-northeast-2.rds.amazonaws.com -e "${SQL_COMMANDS}"
password

