var fs = require('fs');
var path = require('path');

exports.pull = function(pool, table, filter_map, exception_handler, callback) {
  pool.getConnection(function(err, connection) {
    if (err || !connection) {
      console.log('数据表：' + table + ' 连接失败！');
    } else {
      console.log('数据表：' + table + ' 连接成功...');
    }

    var $sql = {
      queryAll: 'select * from ' + table
    };

    connection.query($sql.queryAll, function(err, result) {
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
    // 将数据中出现的 不合法的引号 统统替换为 中文引号
    var dataStr = String(originData).replace(/\\\\\"([^\"]+)\\\\\"/gi, '“$1”');

    // 去除空格和制表符
    dataStr = dataStr.replace(/(\s+|\t+)/gi, '');

    // 去除 JSON数组 的 外层方括号
    dataStr = dataStr.substring(1, dataStr.length-1);

    // 给每一条数据添加配置信息
    var resData = dataStr.replace(/(\{\"id\"\:)(\d+)/gi, '{"delete":{"_index":"parks","_type":"developer","_id":"$2"}},{"create":{"_index":"parks","_type":"developer","_id":"$2"}},$1"$2"');

    // 对已定义的异常数据进行批处理
    if (exception_handler && Object.keys(exception_handler).length > 0) {
      for (var field in exception_handler) {
        for (var i=0;i<exception_handler[field].length;i++) {
          var valPatt = exception_handler[field][i].match;
          var valExs = exception_handler[field][i].writeAs;
          var namePatt = String(field).replace(/(\_|\-)/gi, '\\$1');
          var curPattStr = '/\\"' + namePatt + '\\"\\:\\"' + String(valPatt).replace(/\/([^\/]+)\/g?i?/g, '$1') + '\\"/gi';

          resData = resData.replace(eval(curPattStr), function (word){
            if ( typeof(valExs) == 'string' || typeof(valExs) == 'function' ) {
              var valStr = word.split(':')[1].replace(valPatt, valExs);
              word = word.split(':')[0] + ':' + valStr;
            } else {
              var writeAs = String(valExs);
              word = word.split(':')[0] + ':' + writeAs;
            }
              
            return word;
          });
        }
      }
    }
    
    // 将数据中出现的 不规范的时间字符串 统统处理为 时间戳
    var pattForType_one = /(\"\d\d\d\d\-\d\d\-\d\d)\s?(\d\d\:\d\d(\:\d\d)?\")/gi;
    resData = resData.replace(pattForType_one, '$1 $2');

    resData = resData.replace(pattForType_one, function (item){
      var timeStr = item.substring(1, item.length-1);
      return '"' + String(Date.parse(timeStr)) + '"';
    });

    var pattForType_two = /\"\d\d\d\d\.\d+(\.\d+)?\"/gi;
    resData = resData.replace(pattForType_two, function (item){
      var timeStr = item.substring(1, item.length-1);
      return '"' + String(Date.parse(timeStr)) + '"';
    });

    // 去除文本中 令人头疼的反斜杠
    resData = resData.replace(/(\"newsTitle\"\:\"[^\"\\]+)\\([^\"\\]+\")/gi, '$1$2');
    resData = resData.replace(/\\“([^\\]+)\\”/gi, '“$1”');
    
    // 处理所有数据为一个 JSON数组
    resData = '[' + resData.replace(/\}\{/gi, '},{') + ']';

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
    resData = resData.replace(/(\:)\"\"/g, '$1null');

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

      // 清空本地文件
      function delBulkFile() {
        fs.unlinkSync(path.join(__dirname, '../lib/', es_config.src_table + '.bulk.json'));
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
  });
};