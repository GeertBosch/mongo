/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/update/set_node.h"

#include "mongo/db/update/path_support.h"
#include "mongo/util/assert_util.h"

namespace mongo {

Status SetNode::init(BSONElement modExpr, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    invariant(modExpr.ok());

    _val = modExpr;

    return Status::OK();
}

ModifierNode::ModifyResult SetNode::updateExistingElement(
    mutablebson::Element* element, std::shared_ptr<FieldRef> elementPath) const {
    // If 'element' is deserialized, then element.getValue() will be EOO, which will never equal
    // _val.
    if (element->getValue().binaryEqualValues(_val)) {
        return ModifyResult::kNoOp;
    } else {
        invariant(element->setValueBSONElement(_val));
        return ModifyResult::kNormalUpdate;
    }
}

void SetNode::setValueForNewElement(mutablebson::Element* element) const {
    invariant(element->setValueBSONElement(_val));
}

void SetNode::setValueForNewElementWithMods(const BSONObj* document,
                                            const mutablebson::Element* element,
                                            std::vector<UpdateModification>* mods) const {
    // End offset of the document
    std::size_t bsonEnd = document->objsize() - 1;

    BSONObjBuilder bob;
    bob.appendAs(_val, element->getFieldName());

    BSONObj newObj = bob.obj();

    // The first 4 bytes of the new document (the size) need to be skipped.
    int sizeLen = sizeof(std::uint32_t);
    const char* newElem = newObj.objdata() + sizeLen;
    std::uint32_t newElemLen = newObj.objsize() - sizeLen - 1;

    // Create modify structure to replace the entire document size with the added size of
    // the new elements.
    std::uint32_t newDocSize =
        newElemLen + *reinterpret_cast<const std::uint32_t*>(document->objdata());
    mods->emplace_back(UpdateModification::Buffer(&newDocSize, sizeLen), 0, sizeLen);

    // Create modify structure to insert the new element at the end.
    mods->emplace_back(UpdateModification::Buffer(newElem, newElemLen), bsonEnd, 0);
}

}  // namespace mongo
