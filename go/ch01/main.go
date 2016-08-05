package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	ONE_WEEK_IN_SECONDS int64 = 7 * 86400
	VOTE_SCORE          int64 = 432
)

// get_id_in_key() gets the id part of given key which use colon as separator.
//
// Params:
//   key: key name using colon as separaor. Ex: "user:83271", "article:92617".
// Return:
//   id: id part of the key.
func get_id_in_key(key string) (id string) {
	delimiter := ":"

	idx := strings.LastIndex(key, ":")
	if idx == -1 {
		return ""
	}

	return key[idx+len(delimiter):]
}

// article_vote() is Golang version of Listing 1.6.
func article_vote(conn redis.Conn, user, article string) {
	cutoff := time.Now().Unix() - ONE_WEEK_IN_SECONDS
	t, _ := redis.Int64(conn.Do("ZSCORE", "time:", article))
	if t < cutoff {
		return
	}

	article_id := get_id_in_key(article)
	conn.Do("SADD", "voted:"+article_id, user)
	conn.Do("ZINCRBY", "score:", article, VOTE_SCORE)
	conn.Do("HINCRBY", article, "votes", 1)
}

func post_article(conn redis.Conn, user, title, link string) (article_id string) {
	n, _ := redis.Int64(conn.Do("INCR", "article:"))
	article_id = strconv.FormatInt(n, 10)

	voted := "voted:" + article_id
	conn.Do("SADD", voted, user)
	conn.Do("EXPIRE", "voted", ONE_WEEK_IN_SECONDS)

	now := time.Now().Unix()
	article := "article:" + article_id
	conn.Do("HMSET", article, "title", title, "link", link, "poster", user, "time", now, "votes", 1)

	conn.Do("ZADD", "score:", article, now+VOTE_SCORE)
	conn.Do("ZADD", "time:", article, now)

	return article_id
}

func main() {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("redis.Dial() error: %v\n", err)
		return
	}

	article_id := post_article(conn, "username", "A title", "http://www.google.com")
	fmt.Printf("We posted a new article with id:%v\n", article_id)
	fmt.Printf("Its HASH looks like:\n")
	r, _ := redis.StringMap(conn.Do("HGETALL", "article:"+article_id))
	fmt.Printf("article:%v\n", r)
}
