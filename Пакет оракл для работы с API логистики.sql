/*Настройка прав доступа для http ресурса*/
execute  dbms_network_acl_admin.create_acl ('LogisticsApi.xml', 'Logistics API Access', 'SYSDBA', true, 'connect'); 
execute  dbms_network_acl_admin.assign_acl ('LogisticsApi.xml', 'api.svelnet.com'); 
commit;

/*Проверка, что права настроены*/  
select * from dba_network_acls;

/*Интерфейс пакета*/  
create or replace package LogisticsApi 
as
	/*URL Адрес API*/  
    endpoint_url constant varchar2(128) := 'http://api.svelnet.com:9856/api/logistics/';
	
	/*Код исключения, выбрасываемый при возникновении ошибок в работе API логистики*/  
    logistics_exception_code constant integer := -20001;
	
	/*Возвращаемый тип LogisticsRoute - маршрут в логистике.*/  
    type LogisticsRoute is record(
		RouteID integer, 			-- ID маршрута.
		PointA varchar2(1024), 		-- Точка отправления.
		PointB varchar2(1024),		-- Точка назначения.
		TariffID integer,			-- ID Тарифа
		Transport varchar2(1024),	-- Тип транспорта.
		Price number(19,4),			-- Цена.
		DT date,					-- Дата начала действия.
		ManagerLogin varchar2(256)	-- Пользователь, обновивший тариф.
	);
	
	/*Таблица маршрутов.*/  
    type LogisticsRouteTable is table of LogisticsRoute;

	/*Получение текущих маршрутов*/
    function GetLogisticsRoutes return LogisticsRouteTable pipelined;
	
    /*Отправка в API конфигуратора обработанной заявки на расчет стоимости маршрута.*/
    procedure SendLogisticsRequestResult (request XmlType, logistics_calc_request_id number);
end LogisticsApi;create or replace package LogisticsApi 
as
	/*URL Адрес API*/  
    endpoint_url constant varchar2(128) := 'http://api.svelnet.com:9856/api/logistics/';

	/*Код исключения, выбрасываемый при возникновении ошибок в работе API логистики*/  
    logistics_exception_code constant integer := -20001;

	/*Возвращаемый тип LogisticsRoute - маршрут в логистике.*/  
    type LogisticsRoute is record(
		RouteID integer, 			-- ID маршрута.
		PointA varchar2(1024), 		-- Точка отправления.
		PointB varchar2(1024),		-- Точка назначения.
		TariffID integer,			-- ID Тарифа
		Transport varchar2(1024),	-- Тип транспорта.
		Price number(19,4),			-- Цена.
		DT date,					-- Дата начала действия.
		ManagerLogin varchar2(256)	-- Пользователь, обновивший тариф.
	);
    
	/*Таблица маршрутов.*/  
    type LogisticsRouteTable is table of LogisticsRoute;

	/*Получение текущих маршрутов*/
    function GetLogisticsRoutes return LogisticsRouteTable pipelined;
    
    /*Отправка в API конфигуратора обработанной заявки на расчет стоимости маршрута.*/
    procedure SendLogisticsRequestResult (request XmlType, logistics_calc_request_id number);
end LogisticsApi;

create or replace package body LogisticsApi
is
	/*Функция для работы с HTTP API - принимает наименование конечной точки и возвращает XMLType объект */  
    function GetApiResponse(endpoint_name varchar2) return XMLType
    as
        request               utl_http.req;
        response              utl_http.resp;
        request_body          varchar2(32767);
        response_text         varchar2(30000);
        response_text_buffer  pls_integer := 10000;
        response_clob         clob;
        xml_response          XMLType;
        api_unavailable       exception;
        pragma exception_init (api_unavailable, -12541); -- исключение будет выброшено если сервис недоступен
    begin
        utl_http.set_response_error_check (true);
        utl_http.set_detailed_excp_support (true);       
        dbms_lob.createtemporary(response_clob, false);

        request := utl_http.begin_request(concat(endpoint_url, endpoint_name), 'GET', 'HTTP/1.1');
        utl_http.set_body_charset(request, 'UTF-8');
        utl_http.set_header(request, 'Accept',  'application/xml'); -- просим api предоставить нам данные в XML
        response := utl_http.get_response(request);  

        begin
            loop
              utl_http.read_text(response, response_text, response_text_buffer); 
              dbms_lob.writeappend(response_clob, LENGTH(response_text), response_text);
            end loop;
        exception
            when utl_http.end_of_body then
				utl_http.end_response(response);
        end;

        xml_response := XMLType(response_clob);	
        dbms_lob.freetemporary(response_clob);

        return xml_response;
     exception
        when api_unavailable then
            raise_application_error(logistics_exception_code, 'API логистики недоступно');
        when others then
            dbms_lob.freetemporary(response_clob);
            utl_http.end_response(response);
            raise;  
    end GetApiResponse;
    
    /*Функция для работы с HTTP API - отправка POST запроса. */  
    function MakePostRequest(endpoint_name varchar2, obj XMLType ) return XMLType
    as
        request               utl_http.req;
        response              utl_http.resp;
        request_body_raw      raw(1000);
        response_text         varchar2(30000);
        response_text_buffer  pls_integer := 10000;
        response_clob         clob;
        xml_response          XMLType;
        api_unavailable       exception;
        pragma exception_init (api_unavailable, -12541); -- исключение будет выброшено если сервис недоступен
    begin
        utl_http.set_response_error_check (true);
        utl_http.set_detailed_excp_support (true);       
        dbms_lob.createtemporary(response_clob, false);

        request := utl_http.begin_request(concat(endpoint_url, endpoint_name), 'POST', 'HTTP/1.1');
        request_body_raw := UTL_I18N.STRING_TO_RAW(obj.getstringval(), 'UTF8');    
         
        utl_http.set_body_charset(request, 'UTF-8');
        utl_http.set_header(request, 'Accept',  'application/xml'); -- просим api предоставить нам данные в XML
        utl_http.set_header(request, 'content-type', 'application/xml'); 
        utl_http.set_header(request, 'Content-Length', utl_raw.length(request_body_raw)); -- приходится считать через raw, т.к. content-length в байтах, а в БД может быть разный encoding
        utl_http.write_raw(request, request_body_raw);
        
        response := utl_http.get_response(request);  

        begin
            loop
              utl_http.read_text(response, response_text, response_text_buffer); 
              dbms_lob.writeappend(response_clob, length(response_text), response_text);
            end loop;
        exception
            when utl_http.end_of_body then
				utl_http.end_response(response);
        end;

        xml_response := XMLType(response_clob);	
        dbms_lob.freetemporary(response_clob);

        return xml_response;
     exception
        when api_unavailable then
            raise_application_error(logistics_exception_code, 'API логистики недоступно');
        when others then
            dbms_lob.freetemporary(response_clob);
            utl_http.end_response(response);
            raise;  
    end MakePostRequest;

	/*Получение текущих маршрутов*/
    function GetLogisticsRoutes return LogisticsRouteTable  pipelined
    as
        xml_response XMLType;
        exception_text varchar2(2048);
    begin
        xml_response := GetApiResponse('GetRoutes');

		-- ответ api может содержать текст внутренней ошибки - выводим ее.
        select extractvalue(xml_response, 'ApiResponse/ExceptionMessage') into exception_text from dual;       
        if exception_text is not null then
            raise_application_error(logistics_exception_code, exception_text);
        end if;

        for route in (
            select 
                cast(extractvalue(value(d),'Data/RouteID') as integer) RouteID,
                cast(extractvalue(value(d),'Data/PointA/Name') as varchar2(1024)) PointA,
                cast(extractvalue(value(d),'Data/PointB/Name') as varchar2(1024)) PointB,
                cast(extractvalue(value(t),'LogisticsTariff/TariffID') as integer) TariffID,
                cast(extractvalue(value(t),'LogisticsTariff/Transport/Name') as varchar2(1024)) Transport,
                to_number(extractvalue(value(t),'LogisticsTariff/Price'), '999999999.9999', 'NLS_NUMERIC_CHARACTERS='',.''') as Price, -- подмена разделителя дробной части на точку.
                cast(to_timestamp(extractvalue(value(t),'LogisticsTariff/DT'), 'YYYY-MM-DD"T"HH24:MI:SS.ff3') as date) DT,
                cast(extractvalue(value(t),'LogisticsTariff/ManagerLogin') as varchar2(256)) ManagerLogin
            from table(xmlsequence(extract(xml_response, 'ApiResponse/Data'))) d
                join table(xmlsequence(extract(xml_response, 'ApiResponse/Data/Tariffs/LogisticsTariff'))) t 
                    on extractvalue(value(d),'Data/RouteID') = extractvalue(value(t),'LogisticsTariff/RouteID')
        ) loop
            pipe row (route);
        end loop;
    end GetLogisticsRoutes;   
    
    /*Отправка в API конфигуратора обработанной заявки на расчет стоимости маршрута.*/
    procedure SendLogisticsRequestResult (request XmlType, logistics_calc_request_id number ) 
    as
        xml_response            XMLType;
        exception_text          varchar2(2048);
        response_text           varchar2(512);
        configurator_exception  exception;
        error_code              number;
        error_msg               varchar2(64);
        pragma exception_init(configurator_exception, -20002);
    begin       
        xml_response := MakePostRequest('AddCostCalculationRequest', request);
        
        insert into LogsiticsCalcRequestLog(LogsiticsCalcRequestLogID, LogisticsCalcRequestID, Message)
        values (LogsiticsRequestLogSequence.nextval, logistics_calc_request_id, 'Сообщение отправлено в конфигуратор');
        
        -- ответ api может содержать текст внутренней ошибки - выводим ее.
        select extractvalue(xml_response, 'ApiResponse/ExceptionMessage') into exception_text from dual;       
        if exception_text is not null then
            update LogisticsCalcRequest cr
                set cr.StatusCode = 'Error'                
            where cr.LogisticsCalcRequestID = logistics_calc_request_id;
            insert into LogsiticsCalcRequestLog(LogsiticsCalcRequestLogID, LogisticsCalcRequestID, Message)
            values (LogsiticsRequestLogSequence.nextval, logistics_calc_request_id, exception_text);
        else
            select extractvalue(xml_response, 'ApiResponse/Data') into response_text from dual;
            insert into LogsiticsCalcRequestLog(LogsiticsCalcRequestLogID, LogisticsCalcRequestID, Message)
            values (LogsiticsRequestLogSequence.nextval, logistics_calc_request_id, response_text);
        end if;    
      exception     
        when others then
            error_code := SQLCODE;
            error_msg := substr(SQLERRM, 1 , 64);     
            insert into LogsiticsCalcRequestLog(LogsiticsCalcRequestLogID, LogisticsCalcRequestID, Message)
            values (LogsiticsRequestLogSequence.nextval, logistics_calc_request_id, concat('При отправке сообщения произошла ошибка! - ', concat(error_code, error_msg)));
            raise; 
    end SendLogisticsRequestResult;
end LogisticsApi;

/*Проверка разделителя дробной части decimal*/
select value
from nls_session_parameters
where parameter = 'NLS_NUMERIC_CHARACTERS';

/*Проверка запроса маршрутов логистики.*/
select * from table(LogisticsApi.GetLogisticsRoutes)
