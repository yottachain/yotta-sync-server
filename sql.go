package ytsync

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

//SQLDao dao struct for sql operation
type SQLDao struct {
	db *sqlx.DB
}

//NewDao create a new mysql DAO
func NewDao(url string) (*SQLDao, error) {
	entry := log.WithFields(log.Fields{Function: "NewDao"})
	//url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, ip, port, dbname)
	db, err := sqlx.Connect("mysql", url)
	if err != nil {
		entry.Errorf("create DAO failed: %s", url)
		return nil, err
	}
	return &SQLDao{db: db}, nil
}

//FindCheckPoint find checkpoint record by SN ID
func (sqlDao *SQLDao) FindCheckPoint(id int32) (*CheckPoint, error) {
	entry := log.WithFields(log.Fields{Function: "FindCheckPoint", SNID: id})
	var checkPoint CheckPoint
	err := sqlDao.db.Get(&checkPoint, fmt.Sprintf("SELECT `id`, `start`, `timestamp` FROM checkpoint where id=%d", id))
	if err != nil {
		if err == sql.ErrNoRows {
			entry.WithError(err).Error("no checkpoint found")
		} else {
			entry.WithError(err).Error("find checkpoint")
		}
		return nil, err
	}
	return &checkPoint, nil
}

//InsertCheckPoint insert check point record
func (sqlDao *SQLDao) InsertCheckPoint(checkPoint *CheckPoint) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "InsertCheckPoint", SNID: checkPoint.ID})
	res, err := sqlDao.db.NamedExec("INSERT INTO checkpoint (`id`, `start`, `timestamp`) VALUES (:id, :start, :timestamp)", checkPoint)
	if err != nil {
		entry.WithError(err).Errorf("insert error: start->%d, timestamp->%d", checkPoint.Start, checkPoint.Timestamp)
		return 0, err
	}
	affect, _ := res.RowsAffected()
	return affect, nil
}

//UpdateCheckPoint update check point record
func (sqlDao *SQLDao) UpdateCheckPoint(checkPoint *CheckPoint) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "UpdateCheckPoint", SNID: checkPoint.ID})
	res, err := sqlDao.db.NamedExec("UPDATE checkpoint set `start`=:start, `timestamp`=:timestamp where `id`=:id", checkPoint)
	if err != nil {
		entry.WithError(err).Errorf("prepare error: start->%d, timestamp->%d", checkPoint.Start, checkPoint.Timestamp)
		return 0, err
	}
	affect, _ := res.RowsAffected()
	return affect, nil
}

//InsertBlocks insert blocks into db
func (sqlDao *SQLDao) InsertBlocks(blocks []*Block) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "InsertBlocks"})
	blocksArr := make([][]*Block, 0)
	size := 16000
	for i := 0; i < len(blocks); i += size {
		s := i + size
		if i+size > len(blocks) {
			s = len(blocks)
		}
		blocksArr = append(blocksArr, blocks[i:s])
	}
	var sum int64 = 0
	for _, arr := range blocksArr {
		tx, err := sqlDao.db.Beginx()
		if err != nil {
			entry.WithError(err).Errorf("start transaction error: %d objs", len(blocks))
			if tx != nil {
				tx.Rollback()
			}
			return sum, err
		}
		res, err := tx.NamedExec("INSERT IGNORE INTO blocks (id, vnf, ar, snid) VALUES (:id, :vnf, :ar, :snid)", arr)
		if err != nil {
			entry.WithError(err).Errorf("insert error: %d objs", len(blocks))
			tx.Rollback()
			return sum, err
		}
		err = tx.Commit()
		if err != nil {
			entry.WithError(err).Error("commit failed")
			tx.Rollback()
			return sum, err
		}
		n, _ := res.RowsAffected()
		sum += n
	}
	return sum, nil
}

//InsertShards insert shards into db
func (sqlDao *SQLDao) InsertShards(shards []*Shard) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "InsertShards"})
	shardsArr := make([][]*Shard, 0)
	size := 16000
	for i := 0; i < len(shards); i += size {
		s := i + size
		if i+size > len(shards) {
			s = len(shards)
		}
		shardsArr = append(shardsArr, shards[i:s])
	}
	var sum int64 = 0
	for _, arr := range shardsArr {
		tx, err := sqlDao.db.Beginx()
		if err != nil {
			entry.WithError(err).Errorf("start transaction error: %d objs", len(shards))
			if tx != nil {
				tx.Rollback()
			}
			return sum, err
		}
		res, err := tx.NamedExec("INSERT IGNORE INTO shards (id, nid, vhf, bid) VALUES (:id, :nid, :vhf, :bid)", arr)
		if err != nil {
			entry.WithError(err).Errorf("insert error: %d objs", len(shards))
			tx.Rollback()
			return sum, err
		}
		err = tx.Commit()
		if err != nil {
			entry.WithError(err).Error("commit failed")
			tx.Rollback()
			return sum, err
		}
		n, _ := res.RowsAffected()
		sum += n
	}
	return sum, nil
}

//UpdateShards insert shards into db
func (sqlDao *SQLDao) UpdateShards(metas []*ShardRebuildMeta) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "UpdateShards"})
	metasArr := make([][]*ShardRebuildMeta, 0)
	size := 20000
	for i := 0; i < len(metas); i += size {
		s := i + size
		if i+size > len(metas) {
			s = len(metas)
		}
		metasArr = append(metasArr, metas[i:s])
	}
	var sum int64 = 0
	for _, arr := range metasArr {
		tx, err := sqlDao.db.Beginx()
		if err != nil {
			entry.WithError(err).Error("start transaction error")
			if tx != nil {
				tx.Rollback()
			}
			return sum, err
		}
		pstmt, err := tx.PrepareNamed("UPDATE shards set nid=:nid where id=:vfi and nid=:sid")
		if err != nil {
			entry.WithError(err).Error("prepared failed")
			tx.Rollback()
			return sum, err
		}
		for _, m := range arr {
			res, err := pstmt.Exec(m)
			if err != nil {
				entry.WithError(err).Error("exec failed")
				tx.Rollback()
				return sum, err
			}
			n, _ := res.RowsAffected()
			sum += n
		}
		err = tx.Commit()
		if err != nil {
			entry.WithError(err).Error("commit failed")
			return sum, err
		}
	}
	return sum, nil
}
