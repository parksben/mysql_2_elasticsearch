var fs = require('fs');
var path = require('path');

exports.pull = function(pool, table, sqlPhrase, filter_map, exception_handler, callback) {
  pool.getConnection(function(err, connection) {
    if (err || !connection) {
      console.log('数据表：' + table + ' 连接失败！');
    } else {
      console.log('数据表：' + table + ' 连接成功...');
    }

    var $sql = {};
    if (sqlPhrase.length > 0) {
      $sql.query = sqlPhrase;
    } else {
      $sql.query = 'select * from ' + table;
    }

    connection.query($sql.query, function(err, result) {
      if (err || !result) {
        console.log('数据表：' + table + ' 读取失败！');
      } else {
        console.log('数据表：' + table + ' 读取成功...\n开始从 数据表：' + table + ' 拉取数据，请等待...');
        var resContent = String(JSON.stringify( result ));
        console.log('数据拉取完毕！\n开始转换数据格式...');
        json2bulk(resContent, filter_map, exception_handler);
      }

      connection.release();
    });
  });

  var json2bulk = function(originData, filter_map, exception_handler) {
    // 去除空格和制表符
    var dataStr = String(originData).replace(/(\s+|\t+|\n+|\r+)/gi, '');

    // 给每一条数据添加配置信息
    var dataObj = JSON.parse(dataStr);

    var n = 0;
    while (n < dataObj.length) {
      var cur_id = dataObj[n].id;

      var dataConf = {
        _index: 'parks',
        _type: 'developer',
        _id: cur_id
      };
      dataObj.splice(n, 0, {delete: dataConf}, {create: dataConf});

      n += 3;
    }

    // 对已定义的异常数据进行批处理
    var resData = JSON.stringify(dataObj);

    if (exception_handler && Object.keys(exception_handler).length > 0) {
      var excHandlers = [];
      for (var field in exception_handler) {
        for (var i=0;i<exception_handler[field].length;i++) {
          var valPatt = exception_handler[field][i].match;
          var valExs = exception_handler[field][i].writeAs;
          
          excHandlers.push({
            field_name: field,
            patt_exp: valPatt,
            write_as: valExs
          });
        }
      }

      for (var j=0;j<dataObj.length;j++) {
        if (!dataObj[j].delete && !dataObj[j].create) {
          for (var k=0;k<excHandlers.length;k++) {
            if (!!dataObj[j][excHandlers[k].field_name] && dataObj[j][excHandlers[k].field_name].toString().length > 0) {
              dataObj[j][excHandlers[k].field_name] = dataObj[j][excHandlers[k].field_name].toString().replace(excHandlers[k].patt_exp, excHandlers[k].write_as);
            }
          }
        }
      }

      resData = JSON.stringify(dataObj);
    }
    
    // 将数据中出现的 不规范的时间字符串 统统处理为 时间戳
    var pattForType_one = /(\"\d\d\d\d\-\d\d\-\d\d)\s?(\d\d\:\d\d(\:\d\d)?\")/gi;
    resData = resData.replace(pattForType_one, '$1 $2');

    resData = resData.replace(pattForType_one, function (item){
      var timeStr = item.substring(1, item.length-1);
      return !!Date.parse(timeStr) ? '"' + String(Date.parse(timeStr)) + '"' : 'null';
    });

    var pattForType_two = /\"\d\d\d\d\.\d+(\.\d+)?\"/gi;
    resData = resData.replace(pattForType_two, function (item){
      var timeStr = item.substring(1, item.length-1);
      return !!Date.parse(timeStr) ? '"' + String(Date.parse(timeStr)) + '"' : 'null';
    });

    // 去除文本中 令人头疼的反斜杠
    resData = resData.replace(/(\"newsTitle\"\:\"[^\"\\]+)\\([^\"\\]+\")/gi, '$1$2');
    resData = resData.replace(/\\“([^\\]+)\\”/gi, '“$1”');

    // 去除黑名单字段(river 配置项中的 filter_out)
    if (filter_map && filter_map.length > 0) {
      for (var i=0;i<filter_map.length;i++) {
        var compile_field = filter_map[i].replace(/(\_|\-)/gi, '\\$1');
        var patt_str = '/\"' + compile_field + '\"\:(\"[^\"]+\"|null|\d+|\"\")(\,)?/gi';
        resData = resData.replace(eval(patt_str), '');
        resData = resData.replace(/\,\}/gi, '}');
      }
    }

    // 将字段 “id” 更名
    resData = resData.replace(/\"id\"/gi, '"' + table + '_id"');

    // 将空值赋值为 null
    resData = resData.replace(/(\:)\"(null)?\"/g, '$1null');

    console.log('数据格式转换完成...');
    
    var targetFile = path.join(__dirname, '../lib/', table + '.bulk.json');
    fs.writeFile(targetFile, resData, 'utf8', function (){
      callback({
        table: table,
        message: 'success'
      });
    });
  };
};

exports.push = function(client, es_config, callback) {
  if (!es_config.src_table) {
    es_config.src_table = es_config.type
  }

  fs.readFile(path.join(__dirname, '../lib/', es_config.src_table + '.bulk.json'), 'utf8', function(err, bulkChunk) {
    if (err) {
      console.log(es_config.src_table + '.bulk.json 文件读取错误(T_T)');
    } else {
      console.log('开始导入数据到 /' + es_config.index + '/' + es_config.type + ' 请等待...');
    }

    if (es_config.index && es_config.type) {
      bulkChunk = bulkChunk.replace(/\"\_index\"\:\"[^\"]+\"/gi, '"_index":"' + es_config.index + '"');
      bulkChunk = bulkChunk.replace(/\"\_type\"\:\"[^\"]+\"/gi, '"_type":"' + es_config.type + '"');
    } else {
      console.log('index 或 type 设置有误！');
    }

    var bulkJson = JSON.parse(bulkChunk);
    var chunkSize = !es_config.chunkSize ? 5000 : es_config.chunkSize;

    if (bulkJson.length > chunkSize*3) {
      var chunkNum = Math.ceil(bulkJson.length / (chunkSize*3));
      console.log('--> 共 ' + parseInt(bulkJson.length/3) + ' 条数据，需上传分片：' + chunkNum + ' 个');

      var chunks = [];
      var chunkState = [];
      var requestNum = 0;

      for (var i=0;i<chunkNum;i++) {
        if (i < chunkNum-1) {
          chunks[i] = bulkJson.slice(chunkSize*3*i, chunkSize*3*(i+1));
        } else {
          chunks[i] = bulkJson.slice(chunkSize*3*i);
          importChunk(chunks[requestNum], requestNum);
        }
      }

      // 检验上传结果
      function checkProcess() {
        if (chunkState.join().indexOf('0') < 0) {
          callback({
            index: es_config.index,
            type: es_config.type,
            message: 'success'
          });
        } else {
          callback({
            index: es_config.index,
            type: es_config.type,
            message: 'failed'
          });
        }

        delBulkFile();
      }

      function importChunk(curChunk, i) {
        client.bulk({
          timeout: !es_config.timeout ? '1m' : es_config.timeout,
          body: curChunk
        }, function (err, resp) {
          if (err || !resp) {
            chunkState[i] = '0';
            console.log('----> 上传分片：' + parseInt(i+1) + '/' + chunkNum + ' 失败！');
            console.log(err.message);
          } else {
            chunkState[i] = '1';
            console.log('----> 分片：' + parseInt(i+1) + '/' + chunkNum + ' 上传成功！');
          }

          if (i < chunkNum-1) {
            requestNum += 1;
            importChunk(chunks[requestNum], requestNum);
          } else {
            checkProcess();
          }
        });
      }
    } else {
      console.log('--> 共 ' + parseInt(bulkJson.length/3) + ' 条数据，需上传分片：1 个');

      client.bulk({
        body: bulkJson
      }, function (err, resp) {
        if (err || !resp) {
          callback({
            index: es_config.index,
            type: es_config.type,
            message: 'failed'
          });
        } else {
          callback({
            index: es_config.index,
            type: es_config.type,
            message: 'success'
          });
        }

        delBulkFile();
      });
    }

    // 清空本地文件的方法
    function delBulkFile() {
      fs.unlinkSync(path.join(__dirname, '../lib/', es_config.src_table + '.bulk.json'));
    }
  });
};