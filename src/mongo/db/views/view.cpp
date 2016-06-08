// view.cpp: Non-materialized views

/**
*    Copyright (C) 2016 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/db/views/view.h"

#include <boost/intrusive_ptr.hpp>
#include <memory>

#include "mongo/base/string_data.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/util/log.h"

namespace mongo {

ViewDefinition::ViewDefinition(std::string dbName, std::string viewName, std::string backingViewName, BSONObj& pipeline) {
    _dbName = dbName;
    _viewName = viewName;
    _backingViewName = backingViewName;
    log() << "CREATING: " << dbName << " " << viewName << " " << backingViewName;
    for (BSONElement e : pipeline) {
        BSONObj value = e.Obj();
        _pipeline.push_back(value.copy());
    }
}

BSONObj ViewDefinition::getAggregateCommand(std::string rootNs,
                                            BSONObj& cmd,
                                            std::vector<BSONObj> pipeline) {
    BSONObjBuilder b;
    for (BSONElement e : cmd) {
        StringData fieldName = e.fieldNameStringData();
        if (fieldName == "pipeline") {
            BSONObj p = e.embeddedObject();
            for (BSONElement el : p) {
                pipeline.push_back(el.Obj());
            }
            b.append("pipeline", pipeline);
        } else if (fieldName == "aggregate") {
            size_t dotPos = rootNs.find('.');
            std::string collection = rootNs.substr(dotPos + 1);
            b.append("aggregate", collection);
        } else {
            b.append(e);
        }
    }
    return b.obj();
}

}  // namespace mongo
