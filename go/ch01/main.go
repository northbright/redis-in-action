package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	ONE_WEEK_IN_SECONDS int64  = 7 * 86400
	VOTE_SCORE          int64  = 432
	ARTICLES_PER_PAGE   uint64 = 25
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

// post_article() is Golang version of Listing 1.7.
func post_article(conn redis.Conn, user, title, link string) (article_id string) {
	n, _ := redis.Int64(conn.Do("INCR", "article:"))
	article_id = strconv.FormatInt(n, 10)

	voted := "voted:" + article_id
	conn.Do("SADD", voted, user)
	conn.Do("EXPIRE", voted, ONE_WEEK_IN_SECONDS)

	now := time.Now().Unix()
	article := "article:" + article_id
	conn.Do("HMSET", article, "title", title, "link", link, "poster", user, "time", now, "votes", 1)

	conn.Do("ZADD", "score:", now+VOTE_SCORE, article)
	conn.Do("ZADD", "time:", now, article)

	return article_id
}

// get_articles() is Golang version of Listing 1.8.
func get_articles(conn redis.Conn, page uint64, order string) (articles []map[string]string) {
	start := (page - 1) * ARTICLES_PER_PAGE
	end := start + ARTICLES_PER_PAGE - 1

	ids, _ := redis.Strings(conn.Do("ZREVRANGE", order, start, end))
	articles = []map[string]string{}
	for _, id := range ids {
		article_data, _ := redis.StringMap(conn.Do("HGETALL", id))
		article_data["id"] = id
		articles = append(articles, article_data)
	}

	return articles
}

// add_remove_groups() is Golang version of Listing 1.9.
func add_remove_groups(conn redis.Conn, article_id string, to_add, to_remove []string) {
	article := "article:" + article_id

	for _, v := range to_add {
		redis.Int(conn.Do("SADD", "group:"+v, article))
	}

	for _, v := range to_remove {
		redis.Int(conn.Do("SREM", "group:"+v, article))
	}
}

func main() {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		fmt.Printf("redis.Dial() error: %v\n", err)
		return
	}
	defer conn.Close()

	article_id := post_article(conn, "username", "A title", "http://www.google.com")
	fmt.Printf("We posted a new article with id:%v\n", article_id)
	fmt.Printf("Its HASH looks like:\n")
	r, _ := redis.StringMap(conn.Do("HGETALL", "article:"+article_id))
	fmt.Printf("article:%v\n", r)

	article_vote(conn, "other_user", "article:"+article_id)
	fmt.Printf("We voted for the article, it now has voted:\n")
	v, _ := redis.Uint64(conn.Do("HGET", "article:"+article_id, "votes"))
	fmt.Printf("%v\n", v)

	fmt.Printf("The currently highest-scoring articles are:\n")
	articles := get_articles(conn, 1, "score:")
	fmt.Printf("%v\n", articles)

}
