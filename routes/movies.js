var express = require('express');
var router = express.Router();

// Connect to Movies DB
const { Pool } = require('pg');
const pool = new Pool();

// We can sort by these values
const COLUMN_NAME_LOOKUP = {
	'rating': 'averagerating',
	'genre': 'genres',
	'year': 'endyear',
	'title': 'primarytitle',
	'sort': 'sort',
};

// Query strings
const BASE_QUERY = 'SELECT * FROM userland.titles INNER JOIN userland.ratings ON userland.titles.tconst = userland.ratings.tconst';

const QUERIES = {
	'genre': (count) => {
		return {
			count: count + 1,
			query: ("genres ILIKE '%'||$" + count + "||'%'"),
		};
	},
	'year': (count) => {
		return {
			count: count + 1,
			query: "(startyear = $" + count + " OR (startyear <= $" + count + " AND endyear >= $" + count + "))",
		};
	},
};

const LIMIT_QUERY = "LIMIT 1000";

/* GET by column + filter by query string */
router.get('/:col/:value', function(req, res, next) {
  // Construct SQL query based on col and value
  let { count, query } = QUERIES['' + req.params.col](1);

	sqlQuery = BASE_QUERY + ' WHERE ' + query;
	queryObjectValues = [req.params.value];
	
	// Add any filters to query
	for (let key in req.query) {
		if (key in QUERIES) {
			const queryResults = QUERIES[key](count);
	  	count = queryResults.count;
		
			sqlQuery += ' AND ' + queryResults.query;
			queryObjectValues.push(req.query[key]);
		}
	}

	// Add a sort to the query
	if ('sort' in req.query) {
		// map to known table column
		const column = COLUMN_NAME_LOOKUP[req.query.sort];
		sqlQuery += ' ORDER BY ' + column + ' DESC';
	}

	// Limit the query results, just for performance.
	// You will probably want to remove this for a live application.
	sqlQuery += ' ' + LIMIT_QUERY + ';';

	// Query object safely inserts our values into our SQL query,
	// preventing SQL injections.
	const queryObject = {
  	'text': sqlQuery,
  	'values': queryObjectValues,
	};

  // Send to remote db
  pool.query(queryObject, (qryErr, qryRes) => {
  	// Send results as json to client
  	// TODO: differentiate between connection errors and bad requests
  	if (qryErr) {
  		res.status(400).json({ 'results': 'Bad request' });
  	} else {
			res.json({ 'results': qryRes.rows });	
  	}
  });

  // Bonus Objective:
  // Cache demanding SQL queries with a timeout using Memcached or Redis
  // Redis we can set an expirary time and not worry about manually 
  // invalidating the cache.
  
  // Or we could invalidate the cache with a call to our API (with a secret API
  // key) from the database via a trigger. 
  // Something like this.
  // https://stackoverflow.com/questions/4271812/can-i-execute-a-curl-call-from-postgres-trigger
});

module.exports = router;