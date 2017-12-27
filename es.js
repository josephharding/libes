
const axios = require('axios');
const util = require('util');

const bulk_create_doc = (base_url, index, type, ids, docs) => {
	// on each scroll batch execute this code
	let bulk_body = '';
	for (let i = 0; i < docs.length; i++) {
		console.log("doc:", docs[i]);
		// instead of posting one-by-one we're creating bulk payloads
	  let index_body = {
				_index: index,
				_type: type
    };
    if(ids.length > i) {
      index_body['_id'] = ids[i];
    } 
    bulk_body += JSON.stringify({
			index: index_body
		}) + '\n';
		bulk_body += JSON.stringify(docs[i]) + '\n';
	}	
	return axios.post(`${base_url}/${index}/${type}/_bulk`, bulk_body)
	.then(resp => {
		console.log(`batch status: ${resp.status}, errors: ${resp.data.errors}`);
		if (resp.data.errors) {
			for(let item of resp.data.items) {
				console.log("item:", item);
			}
		}
	});
};

const get_all_docs = (base_url, index, body, batch_delay, on_batch) => {
	return new Promise((resolve, reject) => {
    let url = `${base_url}/${index}/_search?scroll=1m`;
    axios({ method: 'GET', url: url, data: body })	
		.then(resp => {
			scroll(base_url, resp.data._scroll_id, batch_delay, on_batch, () => {
        console.log("finished getting all docs");
        resolve();
      });
		})
		.catch(err => {
		  console.log("error:", util.inspect(err, false, null));	
      reject(err);
		});
	});
};

const migrate = (from_url, from_index, from_type, to_url, to_index, to_type, transform_doc, scroll_delay) => {
  let body = {
    sort: [ "_doc" ]
  };
  let url = `${from_url}/${from_index}/${from_type}/_search?scroll=5m`;
  return new Promise((resolve, reject) => {
    axios({ method: 'GET', url: url, data: body })
    .then(resp => {
      scroll(from_url, resp.data._scroll_id, scroll_delay, (batch_results) => {
        Promise.all(batch_results.map(batch_member => transform_doc(batch_member)))
          .then(populated_batch_results => {

          // on each scroll batch execute this code
          let bulk_body = '';
          for (let batch_member of populated_batch_results) {
            // instead of posting one-by-one we're creating bulk payloads
            bulk_body += JSON.stringify({
              create: {
                _index: to_index,
                _type: to_type,
                _id: batch_member._id
              }
            }) + '\n';
            bulk_body += JSON.stringify(batch_member._source) + '\n';
          }
          let sort_begin = batch_results[0].sort[0];
          let sort_end = batch_results[batch_results.length - 1].sort[0];
          console.log(`sending scroll batch ${sort_begin} to ${sort_end} to index ${to_index}...`);
          axios({ method:'post',
                  url: `${to_url}/${to_index}/${to_type}/_bulk`,
                  data: bulk_body,
                  headers: { 'Content-type': 'application/x-ndjson'}})
            .then(resp => {
              console.log(`scroll batch status: ${resp.status}, errors: ${resp.data.errors}`);
              if (resp.data.errors) {
                for(let item of resp.data.items) {
                  console.log("item:", util.inspect(item, false, null));
                }
              }
            })
            .catch(err => {
              console.log(err);
              reject('bulk error');
            });
          });
      }, () => {
        console.log("finished migration");
        resolve();
      });
    });
  });
};

const scroll = (base_url, scroll_id, delay, onBatch, onComplete) => {
  let body = {
    scroll: "1m",
    scroll_id: scroll_id
  };
  axios.post(`${base_url}/_search/scroll`, body)
  .then(p_resp => {
    if (p_resp.data && p_resp.data.hits && p_resp.data.hits.hits) {
      let search_results = p_resp.data.hits.hits
      console.log("search results length:", search_results.length);
      if (search_results.length > 0) {
        return Promise.all([Promise.resolve(p_resp.data._scroll_id)].concat(onBatch(search_results)));
      } else {
        console.log("empty hits found!");
        onComplete();
      }
    } else {
      console.log("no more hits found!");
      onComplete();
    }
  })
  .then(([scroll_id, ...res]) => {
    console.log(`waiting for ${delay} ms before next batch....`); 
    setTimeout(() => { 
      scroll(base_url, scroll_id, delay, onBatch, onComplete);
    }, delay);
  })
  .catch(err => {
    console.log("scroll error:", err);
  });
}

const createDoc = (base_url, index, type, doc) => {
  return axios.post(`${base_url}/${index}/${type}/`, doc)
    .then((res) => {
      if (res.data) {
        return res.data._id
      } else {
        return null; 
      }
    })
    .catch(e => console.log("ERROR:", e));
};

const createDocWithId = (base_url, index, type, id, doc) => {
  return axios.post(`${base_url}/${index}/${type}/${id}`, doc)
    .then((res) => {
      if (res.data) {
        return res.data._id
      } else {
        return null; 
      }
    })
    .catch(e => console.log("ERROR:", e));
};

const deleteDoc = (base_url, index, type, doc) => {
  return axios.delete(`${base_url}/${index}/${type}/${doc}`)
    .catch(e => console.log("error:", __filename, "LINE_N", e));
};

const getDoc = (base_url, index, type, id) => {
  return axios.get(`${base_url}/${index}/${type}/${id}`)
};

const deleteIndex = (base_url, index) => {
  return axios.delete(`${base_url}/${index}`)
    .catch(e => console.log("error:", __filename, "LINE_N", e));
};

const query = (base_url, index, dsl) => {
  return axios({
    method: 'GET',
    url: `${base_url}/${index}/_search?q=${dsl}`
  })
  .then(res => res.data)
  .catch(e => console.log("error:", __filename, "LINE_N", e));
};

const search = (base_url, index, data) => {
  return axios({
    method: 'GET',
    url: `${base_url}/${index}/_search`,
    data: data
  })
  .then(res => res.data)
  .catch(e => console.log("error:", __filename, "LINE_N", util.inspect(e, false, null)));
};

const search_scroll = (base_url, index, body, batch_delay, on_batch) => {
  return new Promise((resolve, reject) => {
    let url = `${base_url}/${index}/_search?scroll=1m`;
    axios({ method: 'GET', url: url, data: body })
    .then(resp => {
      scroll(base_url, resp.data._scroll_id, batch_delay, on_batch, () => {
        resolve();
      });
    })
    .catch(err => {
      console.log("error:", util.inspect(err, false, null));
      reject(err);
    });
  });
};

const create_index = (base_url, index) => {
  return axios({
    method: 'PUT',
    url: `${base_url}/${index}`
  })
  .then(res => res.data)
  .catch(e => console.log("error:", __filename, "LINE_N", util.inspect(e, false, null)));

};

const create_mapping = (base_url, index, type, mapping) => {
  return axios({
    method: 'PUT',
    url: `${base_url}/${index}/_mapping/${type}`,
    data: mapping
  })
  .then(res => res.data)
  .catch(e => console.log("error:", __filename, "LINE_N", util.inspect(e, false, null)));
};

module.exports = {
  deleteIndex: deleteIndex,
  deleteDoc: deleteDoc,
  createDoc: createDoc,
  createDocWithId: createDocWithId,
	bulk_create_doc: bulk_create_doc,
	get_all_docs: get_all_docs,
  getDoc: getDoc,
  query: query,
	migrate: migrate,
  search: search,
  search_scroll: search_scroll,
  create_index: create_index,
  create_mapping: create_mapping
};
