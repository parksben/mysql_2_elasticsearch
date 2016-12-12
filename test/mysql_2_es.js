var esMysqlRiver = require('mysql_2_elasticsearch');

/*
** 以下为 mysql_2_elasticsearch 的相关参数配置(详情见注释)
*/

var river_config = {

  /* [必需] MySQL数据库的相关参数(根据实际情况进行修改) */
  mysql: {
    host: '127.0.0.1',
    user: 'root',
    password: 'root',
    database: 'users',
    port: 3306
  },

  /* [必需] es 相关参数(根据实际情况进行修改) */
  elasticsearch: {

    host_config: {               // [必需] host_config 即 es客户端的配置参数，详细配置参考 es官方文档[https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html]
      host: 'localhost:9200',
      log: 'trace',
      // Other options...
    },

    index: 'myIndex',            // [必需] es 索引名
    chunkSize: 8000,             // [非必需] 单个数据分片的最大数据量，不设置则默认为 5000 (条数据)
    timeout: '2m'                // [非必需] 单次分片请求的超时时间，不设置则默认为 1m (注：此参数并非es客户端请求的timeout，后者请在 host_config 中设置)
  
  },

  /* [必需] 数据传送的规则 */
  riverMap: {
    'users => users': {          // [必需] 'a => b' 表示将 mysql数据库中名为 'a' 的 table 的所有数据 输送到 es中名为 'b' 的 type 中去
      filter_out: [                // [非必需] 需要过滤的字段名，即 filter_out 中的设置的所有字段将不会被导入 elasticsearch 的数据中
        'password',
        'age'
      ],
      exception_handler: {         // [非必需] 异常处理器，使用JS正则表达式处理异常数据，以避免 es 入库时由于数据类型不合法造成的数据丢失，可根据具体需求进行设置
        'birthday': [                // [示例] 对 users 表的 birthday 字段的异常数据进行处理
          {
            match: /NaN/gi,          // [示例] 正则条件(此例匹配字段值为 "NaN" 的情况)
            writeAs: null            // [示例] 将 "NaN" 重写为 null
          },
          {
            match: /(\d{4})年/gi,    // [示例] 正则表达式(此例匹配字段值为形如 "2016年" 的情况)
            writeAs: '$1.1'          // [示例] 将 "2015年" 样式的数据重写为 "2016.1" 样式的数据
          }
        ]
      }
    },
    // Other fields' options...
  }

};


/*
** 以下代码内容：
** 通过 esMysqlRiver 方法进行数据传输，方法的回调参数(一个JSON对象) obj 包含此次数据传输的结果
** 其中：
** 1. obj.total    => 需要传输的数据表数量
** 2. obj.success  => 传输成功的数据表数量
** 3. obj.failed   => 传输失败的数据表数量
** 4. obj.failed   => 本次数据传输的结论
*/

esMysqlRiver(river_config, function(obj) {
  /* 将传输结果打印到终端 */
  console.log('\n---------------------------------');
  console.log('总传送：' + obj.total + '项');
  console.log('成功：' + obj.success + '项');
  console.log('失败：' + obj.failed + '项');
  if (obj.result == 'success') {
    console.log('\n结论：全部数据传送完成！');
  } else {
    console.log('\n结论：传送未成功...');
  }
  console.log('---------------------------------');
  console.log('\n(使用 Ctrl + C 退出进程)');
  /* 将传输结果打印到终端 */
});