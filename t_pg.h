#ifndef T_PG_H
#define T_PG_H

#include <cstdint>
#include <vector>
#include <memory>

#include <QtCore>
#include <QtEndian>

#include <libpq-fe.h>

inline void close(PGconn* conn) { PQfinish(conn); }

inline void close(PGresult* res) { PQclear(res); }

template<class T>
class Closer {
public:
	Closer(T* p = nullptr) : p_(p) {}
	Closer(Closer&& p) : Closer(p.release()) {}
	Closer& operator = (Closer&& p) { std::swap(p_, p.p_); return *this; }
	~Closer() { if (p_) close(p_); }

	T* release() { auto t = p_; p_ = nullptr; return t; }
	T* get() const { return p_; }

	bool valid() const { return p_ != nullptr; }

	bool operator ! () const { return !valid(); }
 
private:
	Closer(const Closer&) = delete;
	Closer& operator = (const Closer&) = delete;

private:
	T* p_;
};

template<class T>
using PgHandle = Closer<T> ;
 
template<class T> inline
PgHandle<T> makePgHandle(T* p) { return p; }

class SqlParameterList {
public:
	SqlParameterList() : params_(), formats_() {}

	SqlParameterList(const SqlParameterList& p) :
		params_(p.params_), 
		formats_(p.formats_)
	{}

	SqlParameterList(SqlParameterList&& p) :
		params_(std::move(p.params_)),
		formats_(std::move(p.formats_))
		{}

	SqlParameterList& operator = (const SqlParameterList& p) {
		params_ = p.params_;
		formats_ = p.formats_;
		return *this;
	}

	SqlParameterList& operator = (SqlParameterList&& p) {
		params_ = std::move(p.params_);
		formats_ = std::move(p.formats_);
		return *this;
	}

	SqlParameterList& operator += (const SqlParameterList& p) {
		params_.insert(params_.end(), p.params_.begin(), p.params_.end());
		formats_.insert(formats_.end(), p.formats_.begin(), p.formats_.end());
		return *this;
	}


	SqlParameterList& arg(const QDateTime& data) {
		return arg(data.toString("yyyy-MM-dd HH:mm:ss"));
	}

	SqlParameterList& arg(QByteArray&& data) {
		if (validateData(data)) {
			params_.emplace_back(std::move(data));
			formats_.push_back(1);
		}
		return *this;
	}

	SqlParameterList& arg(const QByteArray& data) {
		if (validateData(data)) {
			params_.push_back(data);
			formats_.push_back(1);
		}
		return *this;
	}

	SqlParameterList& arg(const char* data) {
		if (validateData(data)) {
			params_.emplace_back(QByteArray(data));
			formats_.push_back(0);
		}
		return *this;
	}

	SqlParameterList& arg(const QString& data) {
		if (validateData(data)) {
			params_.emplace_back(data.toLocal8Bit());
			formats_.push_back(0);
		}
		return *this;
	}

	SqlParameterList& arg(const std::string& data) {
		return SqlParameterList::arg(data.c_str());
	}

	template<class T>
	SqlParameterList& arg(const T& value) {
		return SqlParameterList::arg(std::to_string(value));
	}

	SqlParameterList& arg(const QVariant& value) {
		return SqlParameterList::arg(value.toString());
	}

	const std::vector<QByteArray>& params() const { return params_; }

	const std::vector<int>& formats() const { return formats_; }

    size_t size() const { return params().size(); }

	void reserve(size_t size) const  {
		const_cast<SqlParameterList*>(this)->reserve(size);
	}

	void reserve(size_t size) {
		params_.reserve(size);
		formats_.reserve(size);
	}


    struct ParamFormat {
        QByteArray param;
        int format;
    };

    std::vector<ParamFormat> paramWithFormat() const {
        std::vector<ParamFormat> result;

        auto nParams = params_.size();

        if( nParams != formats_.size() ) {
            qWarning() << "invalid data";
            return result;
        }

        result.reserve( params_.size() );

        for(size_t i = size_t(); i < params_.size(); ++i ) {
            result.push_back( { params_[i], formats_[i] });
        }

        return result;
    }

private:
	static bool isEmpty(const char* str) {
		return !(str && *str);
	}

	static bool isEmpty(const QByteArray& data) {
		return data.isEmpty();
	}

	static bool isEmpty(const QString& data) {
		return data.isEmpty();
	}

	template<class T>
	static bool validateData(const T& data) {
		bool fail = isEmpty(data);
		if (fail) {
			qWarning() << "error - Invalid SQL argument. Empty data";
		}
		return !fail;
	}

private:
	std::vector<QByteArray> params_;
	std::vector<int> formats_;
};

inline SqlParameterList operator + (const SqlParameterList& a, const SqlParameterList& b) {
	return SqlParameterList(a) += b;
}


// Sql("INSERT INTO table (name, data) VALUES ($1, $2::bytea)").arg(name).arg(data)
class Sql {
public:
	Sql() : command_(), params_() {}
	Sql(const char* cmd) : command_(cmd), params_() {}
	Sql(const std::string& cmd) : command_(QByteArray::fromRawData(cmd.data(), cmd.size())), params_() {}
	Sql(const QByteArray& cmd) : command_(cmd), params_() {}
	Sql(QByteArray&& cmd) : command_(std::move(cmd)), params_() {}
	Sql(const QString& cmd) : command_(cmd.toLocal8Bit()), params_() {}
	Sql(const Sql& sql_) : command_(sql_.command_), params_(sql_.params_) {}
	Sql(Sql&& sql_) :
		command_(std::move(sql_.command_)),
		params_(std::move(sql_.params_)) {}

	Sql& operator = (const Sql& sql_) {
		command_ = sql_.command_;
		params_ = sql_.params_;
		return *this;
	}

	Sql& operator = (Sql&& sql_) {
		command_ = std::move(sql_.command_);
		params_ = std::move(sql_.params_);
		return *this;
	}

	Sql& operator += (const Sql& sql_) {
		command_ += sql_.command_;
		params_ += sql_.params_;
		return *this;
	}

	Sql& operator += (const QByteArray& sql_) {
		command_ += sql_;
		return *this;
	}

	Sql& operator += (QByteArray&& sql_) {
		command_ += std::move(sql_);
		return *this;
	}

	Sql& operator += (const char* sql_) {
		command_ += sql_;
		return *this;
	}

	Sql& operator += (char c) {
		command_ += c;
		return *this;
	}

	template<class T>
	Sql& arg(T&& data) {
		params_.arg(data);
		return *this;
	}

	template<class T>
	Sql& arg(const T& data) {
		params_.arg(data);
		return *this;
	}

	const QByteArray& command() const { return command_; }

	const char* c_command() const { return command(); }

	const SqlParameterList& params() const { return params_; }

	static const char paramPrefix = '$';

	uint32_t parseParamsCount() const {
		return command_.count(paramPrefix);
	}

	bool valid() const {
		auto count = params().size();
        return (
			!command_.isEmpty() &&
			count < INT_MAX &&
			count == params().params().size() &&
			count == params().formats().size() &&
			static_cast<uint32_t>(count) == parseParamsCount()
		);
	}
	
	void debug() const {
        QByteArray debug_(command_);

		qlonglong nParam = 1ULL;
		for (auto& param : params_.paramWithFormat() ) {
			if (!param.format) {
				debug_.replace(paramPrefix + QByteArray::number(nParam), param.param);
			}
			++nParam;
		}
		
		qDebug() << debug_;
	}

private:
	QByteArray command_;
	SqlParameterList params_;
};

inline Sql operator + (const Sql& a, const Sql& b) {
	return Sql(a) += b;
}

template<class T, class Fn> inline
auto v_convert(const std::vector<T>& v, Fn fn) -> std::vector<decltype(fn(T()))> {
	std::vector<decltype(fn(T()))> res;
	res.reserve(v.size());
	for (auto& item : v) {
		res.emplace_back(fn(item));
	}
	return res;
}


// auto firstRowFirstColumn = value<int>(res, 0, 0);
template<class T> inline
T value(const PGresult* res, uint32_t row, uint32_t column) {
	if (!PQgetisnull(res, row, column) && PQgetlength(res, row, column) == sizeof(T)) {
		return qFromBigEndian<T>(reinterpret_cast<const uchar*>(PQgetvalue(res, row, column)));
	}
	return{};
}

template<> inline
QString value<QString>(const PGresult* res, uint32_t row, uint32_t column) {
	return (!PQgetisnull(res, row, column)) ? 
		QString::fromLocal8Bit(
			PQgetvalue(res, row, column), 
			PQgetlength(res, row, column)
		) : QString();
}

template<> inline
QByteArray value<QByteArray>(const PGresult* res, uint32_t row, uint32_t column) {
	return (!PQgetisnull(res, row, column)) ? 
		QByteArray::fromRawData(
			PQgetvalue(res, row, column), 
			PQgetlength(res, row, column)
		) : QByteArray();
}

template<> inline
bool value<bool>(const PGresult* res, uint32_t row, uint32_t column) {
	return *PQgetvalue(res, row, column) != '\0';
}


template<> inline
QDateTime value<QDateTime>(const PGresult* res, uint32_t row, uint32_t column) {
	return QDateTime(QDate(2000, 1, 1), QTime(0, 0, 0))
		.addMSecs(::value<int64_t>(res, row, column) / 1000);
}

inline QString errorMessage(const PGconn* conn_) {
	QString message;

	if (!conn_) {
		message = "PgClient - invalid connection handle";
	} else if (PQstatus(conn_) != CONNECTION_OK) {
		message = QString("PGconn - ") + QString(PQerrorMessage(conn_));
	}

	if (!message.isEmpty()) {
		qWarning() << message;
	}

	return message;
}

class PgResult;

class PgRowColumn {
public:
	PgRowColumn(const PgResult* result, uint32_t row, uint32_t column) :
		result_(result),
		row_(row),
		column_(column) {}

	template<class T>
	inline T to() const;

	PgRowColumn& next() { ++row_; return *this;  }
	PgRowColumn& operator * () { return *this; }
	PgRowColumn& operator ++ () { return next(); }

	bool operator == (const PgRowColumn& value)  const { return column_ == value.column_; }
	bool operator != (const PgRowColumn& value)  const { return !(*this == value); }

private:
	const PgResult* result_;
	uint32_t row_;
	uint32_t column_;
};

class PgRow {
public:
    PgRow() : result_(nullptr), row_(0UL) {}

	PgRow(const PgResult* result, uint32_t row) : result_(result), row_(row) {}

	PgRowColumn column(uint32_t column) const { return at(column); }
	
	PgRowColumn value(uint32_t column) const { return at(column); }

	template<class T> inline
	T value(uint32_t column) const { return at(column).to<T>(); }

	PgRowColumn at(uint32_t column) const { return PgRowColumn(result_, row_, column); }
	PgRowColumn operator [] (uint32_t column) const { return at(column); }
	PgRowColumn begin() const { return at(0); }
	PgRowColumn end() const { return at(size()); }
	inline uint32_t size() const;
	bool empty() const { return size() == 0LL; }
	bool valid() const { return !empty(); }
	PgRow& next() { ++row_; return *this; }
	PgRow& operator * () { return *this; }
	PgRow& operator ++ () { return next();}
	bool operator == (const PgRow& value)  const { return row_ == value.row_; }
	bool operator != (const PgRow& value)  const { return !(*this == value); }

private:
	const PgResult* result_;
	uint32_t row_;
};

class PgResult  {
public:
	PgResult() : res_(), n_rows_(0UL), n_columns_(0UL) {}

	PgResult(PgHandle<PGresult>&& res) :
		res_(std::move(res)),
		n_rows_(0UL),
		n_columns_(0UL)
	{
		if (res_.valid()) {
			const int n_rows{ PQntuples(res_.get()) };
			const int n_coumns{ PQnfields(res_.get()) };
			if (n_rows < 0 || n_coumns < 0) {
				qWarning() << "invalid SQL result: tuples count or fields count < 0";
				res_ = nullptr;
			}
			else {
				n_rows_ = n_rows;
				n_columns_ = n_coumns;
			}
		}
	}

	PgResult(PgResult&& res) :
		res_(std::move(res.res_)),
		n_rows_(res.n_rows_),
		n_columns_(res.n_columns_) {}

	PgResult& operator = (PgResult&& res) {
		res_ = std::move(res.res_);
		n_rows_ = res.n_rows_;
		n_columns_ = res.n_columns_;
		return *this;
	}

	uint32_t rowCount() const { return n_rows_; }

	uint32_t columnCount() const { return n_columns_; }

	bool valid() const { return res_.valid(); }

	bool operator !() const { return !valid(); }
 
	PGresult* get() const { return res_.get(); }

	PgRow row(uint32_t index) const { return at(index); }

	PgRow value(uint32_t index) const { return at(index); }

	PgRowColumn value(uint32_t row, uint32_t column) const {
		return value(row).value(column); 
	}

	template<class T>
	PgRowColumn value(uint32_t row, uint32_t column) const {
		return value(row).value<T>(column);
	}

	PgResult& rows() { return *this; }

	uint32_t size() const { return rowCount(); }
	uint32_t empty() const { return size() == 0LL; }
	PgRow at(uint32_t index) const {
		return (index < size()) ? PgRow(this, index) : PgRow();
	}
	PgRow begin() const { return PgRow(this, 0UL); }
	PgRow end() const { return PgRow(this, size()); }
	PgRow front() const { return at(0UL); }
	PgRow back() const { return at(size() - 1); }
	PgRow operator [] (uint32_t index) const {
		return PgRow(this, index);
	}

private:
	PgResult(const PgResult& res) = delete;
	PgResult& operator = (const PgResult& res) = delete;

private:
	PgHandle<PGresult> res_;
	uint32_t n_rows_;
	uint32_t n_columns_;
};

template<class T> inline
T PgRowColumn::to() const {
	return (result_ && column_ < result_->columnCount()) ?
		::value<T>(result_->get(), row_, column_) : T();
}

inline uint32_t PgRow::size() const {
	return (result_) ? result_->columnCount() : 0UL;
}

inline PgHandle<PGresult> exec(PGconn* conn, const Sql& sql_, QString* error = nullptr) {
    auto errorReport = [error](const QString& message) {
		qWarning() << message;
		if (error) {
			*error = message;
		}
		return nullptr;
	};

	auto isFailStatus = [](ExecStatusType status) {
		return (status != PGRES_COMMAND_OK) && (status != PGRES_TUPLES_OK);
	};

	if (!sql_.valid()) {
		return errorReport("Sql - Too many parameters");
	}

    const auto& params = sql_.params();
    const auto& vparams = params.params();
    const auto n_params = params.size();
    const bool is_params = (n_params > size_t());
	
	sql_.debug();

	auto result = makePgHandle(PQexecParams(
		conn, sql_.c_command(),
        static_cast<int>(n_params),
		nullptr,
		(is_params) ? v_convert(vparams, [](const QByteArray& data) { return data.data(); }).data() : nullptr,
        (is_params) ? v_convert(vparams, [](const QByteArray& data) { return static_cast<int>(data.size()); }).data() : nullptr,
        (is_params) ? params.formats().data() : nullptr,
        1
	));

	if (!result.get()) {
		return errorReport("PGresult - invalid result handle");
	}

	if (isFailStatus(PQresultStatus(result.get()))) {
		return errorReport(QString("PGresult - ") + QString(PQresultErrorMessage(result.get())) );
	}

    return result;
}

class PgConnection {
public:
	PgConnection() : 
		conn_(),
		errorMessage_() {
	}

	PgConnection(const QString& conStr) : 
		conn_(makePgHandle(PQconnectdb(conStr.toLocal8Bit()))),
		errorMessage_() 
	{
		if (validate()) {
			if (PQsetClientEncoding(conn_.get(), "WIN1251") != 0) {
				qWarning() << "error PQsetClientEncoding";
			}
		}
	}

	PgConnection(PgConnection&& rvalue) :
		conn_(std::move(rvalue.conn_)),
		errorMessage_(std::move(rvalue.errorMessage_))
	{}

	PgConnection& operator = (PgConnection&& rvalue) {
		errorMessage_ = std::move(rvalue.errorMessage_);
		conn_ = std::move(rvalue.conn_);
		return *this;
	}

	bool valid() const { return errorMessage_.isEmpty(); }

	bool validate() {
		if (valid()) {
			errorMessage_ = ::errorMessage(conn_.get());
		}
		return valid();
	}

	bool operator ! () const { return !valid(); }

	QString errorMessage() const { return errorMessage_; }

	// exec(Sql("INSERT INTO table (name, data) VALUES ($1, $2::bytea)").arg(name).arg(data))
	PgResult exec(const Sql& sql_) {
        PgResult res;
        if(validate()) {
			res = std::move(::exec(conn_.get(), sql_, &errorMessage_));
        }
		return res;
	}

	PGconn* get() const { return conn_.get(); }

private:
	PgConnection(const PgConnection& res) = delete;
	PgConnection& operator = (const PgConnection& res) = delete;

private:
	PgHandle<PGconn> conn_;
	QString errorMessage_;
};

#endif
