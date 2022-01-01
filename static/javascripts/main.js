/**
 * TODO 本功能需要layer和jquery插件的支持； 本功能为二次开发。
 * @see 源文件地址：http://sc.chinaz.com/jiaobendemo.aspx?downloadid=0201785541739
 */
var layer;
layui.use('layer', function () {
    layer = layui.layer;
});

function main() {
    if (typeof (layer) != "object" || !layer) {
        setTimeout("main()", 400);
        return;
    }
    var myCalendar = new SimpleCalendar('#calendar', {
        width: '100%',
        height: '500px',
        language: 'CH', //语言
        showLunarCalendar: true, //阴历
        showHoliday: false, //休假-暂时禁用
        showFestival: true, //节日
        showLunarFestival: true, //农历节日
        showSolarTerm: true, //节气
        showMark: true, //标记
        realTime: true, //具体时间
        timeRange: {
            startYear: 2002,
            endYear: 2049
        },
        mark: {},
        markColor: ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],//记事各个颜色
        main: function (year, month) {
            // alert("[获取数据]" + year + "--->" + month);
            var index = -1;
            if (layer) index = layer.msg('正在查询数据......', {icon: 16, shade: 0.6});
            //@-这里获取数据：
            console.log(year + "--->" + month);

            //模拟获取数据start
            var resultObj = {}, status = ['待揽收', '已发货', '配送中', '已签收'];
            $.ajaxSettings.async = false;

            $.get("http://119.91.214.221:8888/getTodos/month/" + year + (month < 10 ? "0" : "") + month, function (data) {
                console.log(data)
                var json = data.todos
                for (ith in json) {
                    var array = []

                    date = json[ith].day
                    if (resultObj.hasOwnProperty(date)) {
                        array = resultObj[date]
                    }
                    array.push({
                        id: json[ith].id,
                        title: json[ith].info,
                        name: '某区',
                        ratio: '2%',
                        status: 1,
                        statusText: status[1]
                    });
                    resultObj[date] = array;
                }
                console.log(resultObj);
            });
            if (layer) layer.close(index);
            return resultObj;

        },
        timeupdate: false,//显示当前的时间HH:mm
        theme: {
            changeAble: false,
            weeks: {
                backgroundColor: '#FBEC9C',
                fontColor: '#4A4A4A',
                fontSize: '20px',
            },
            days: {
                backgroundColor: '#ffffff',
                fontColor: '#565555',
                fontSize: '24px'
            },
            todaycolor: 'orange',
            activeSelectColor: 'orange',
        }
    });
}

main();
