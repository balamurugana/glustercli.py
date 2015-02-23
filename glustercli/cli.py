#
# Copyright 2015 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import xml.etree.cElementTree as etree
import ethtool
import socket

import utils

if hasattr(etree, 'ParseError'):
    _etreeExceptions = (etree.ParseError, AttributeError, ValueError)
else:
    _etreeExceptions = (SyntaxError, AttributeError, ValueError)

_glusterCommandPath = utils.CommandPath("gluster",
                                        "/usr/sbin/gluster",
                                        )
_TRANS_IN_PROGRESS = "another transaction is in progress"
_peerUUID = ''
_peer = ''


def _getLocalPeer():
    global _peer

    if _peer:
        return _peer

    fqdn = socket.getfqdn()
    ip = socket.gethostbyname(fqdn)
    if not ip.startswith('127.'):
        _peer = ip
        return ip

    for dev in ethtool.get_active_devices():
        try:
            ip = ethtool.get_ipaddr(dev)
            if not ip.startswith('127.'):
                _peer = ip
                return ip
        except IOError:
            # TODO: log it
            pass

    return fqdn


def _getGlusterVolCmd():
    return [_glusterCommandPath.cmd, "--mode=script", "volume"]


def _getGlusterPeerCmd():
    return [_glusterCommandPath.cmd, "--mode=script", "peer"]


def _getGlusterSystemCmd():
    return [_glusterCommandPath.cmd, "system::"]


def _getGlusterVolGeoRepCmd():
    return _getGlusterVolCmd() + ["geo-replication"]


def _getGlusterSnapshotCmd():
    return [_glusterCommandPath.cmd, "--mode=script", "snapshot"]


class BrickStatus:
    PAUSED = 'PAUSED'
    COMPLETED = 'COMPLETED'
    RUNNING = 'RUNNING'
    UNKNOWN = 'UNKNOWN'
    NA = 'NA'


class HostStatus:
    CONNECTED = 'CONNECTED'
    DISCONNECTED = 'DISCONNECTED'
    UNKNOWN = 'UNKNOWN'


class VolumeStatus:
    ONLINE = 'ONLINE'
    OFFLINE = 'OFFLINE'


class TransportType:
    TCP = 'TCP'
    RDMA = 'RDMA'


class TaskType:
    REBALANCE = 'REBALANCE'
    REPLACE_BRICK = 'REPLACE_BRICK'
    REMOVE_BRICK = 'REMOVE_BRICK'


class GlusterXMLError(Exception):
    message = "XML error"

    def __init__(self, cmd, xml):
        self.cmd = cmd
        self.xml = xml

    def __str__(self):
        return "%s\ncommand: %s\nXML: %s" % (self.message, self.cmd, self.xml)


class GlusterCmdFailed(utils.CmdExecFailed):
    message = "gluster command failed"


class GlusterBusy(utils.CmdExecFailed):
    message = "gluster busy"


def _throwIfBusy(cmd, rc, out, err):
    o = out + err
    if _TRANS_IN_PROGRESS in o.lower():
        raise GlusterBusy(cmd, rc, out, err)


def _execGluster(cmd):
    rc, out, err = utils.execCmd(cmd)
    _throwIfBusy(cmd, rc, out, err)
    return rc, out, err


def _execGlusterXml(cmd):
    cmd.append('--xml')
    rc, out, err = utils.execCmd(cmd)
    _throwIfBusy(cmd, rc, out, err)

    try:
        tree = etree.fromstring(out)
        rv = int(tree.find('opRet').text)
        msg = tree.find('opErrstr').text
        errNo = int(tree.find('opErrno').text)
    except _etreeExceptions:
        raise GlusterXMLError(cmd, out)

    if rv == 0:
        return tree

    if errNo != 0:
        rv = errNo

    raise GlusterCmdFailed(cmd, rv, err=msg)


def _getLocalPeerUUID():
    global _peerUUID

    if _peerUUID:
        return _peerUUID

    command = _getGlusterSystemCmd() + ["uuid", "get"]
    rc, out, err = _execGluster(command)

    o = out.strip()
    if o.startswith('UUID: '):
        _peerUUID = o[6:]

    return _peerUUID


def _parseVolumeStatus(tree):
    status = {'name': tree.find('volStatus/volumes/volume/volName').text,
              'bricks': [],
              'nfs': [],
              'shd': []}
    hostname = _getLocalPeer()
    for el in tree.findall('volStatus/volumes/volume/node'):
        value = {}

        for ch in el.getchildren():
            value[ch.tag] = ch.text or ''

        if value['path'] == 'localhost':
            value['path'] = hostname

        if value['status'] == '1':
            value['status'] = 'ONLINE'
        else:
            value['status'] = 'OFFLINE'

        if value['hostname'] == 'NFS Server':
            status['nfs'].append({'hostname': value['path'],
                                  'hostuuid': value['peerid'],
                                  'port': value['port'],
                                  'status': value['status'],
                                  'pid': value['pid']})
        elif value['hostname'] == 'Self-heal Daemon':
            status['shd'].append({'hostname': value['path'],
                                  'hostuuid': value['peerid'],
                                  'status': value['status'],
                                  'pid': value['pid']})
        else:
            status['bricks'].append({'brick': '%s:%s' % (value['hostname'],
                                                         value['path']),
                                     'hostuuid': value['peerid'],
                                     'port': value['port'],
                                     'status': value['status'],
                                     'pid': value['pid']})
    return status


def _parseVolumeStatusDetail(tree):
    status = {'name': tree.find('volStatus/volumes/volume/volName').text,
              'bricks': []}
    for el in tree.findall('volStatus/volumes/volume/node'):
        value = {}

        for ch in el.getchildren():
            value[ch.tag] = ch.text or ''

        sizeTotal = int(value['sizeTotal'])
        value['sizeTotal'] = sizeTotal / (1024.0 * 1024.0)
        sizeFree = int(value['sizeFree'])
        value['sizeFree'] = sizeFree / (1024.0 * 1024.0)
        status['bricks'].append({'brick': '%s:%s' % (value['hostname'],
                                                     value['path']),
                                 'hostuuid': value['peerid'],
                                 'sizeTotal': '%.3f' % (value['sizeTotal'],),
                                 'sizeFree': '%.3f' % (value['sizeFree'],),
                                 'device': value['device'],
                                 'blockSize': value['blockSize'],
                                 'mntOptions': value['mntOptions'],
                                 'fsName': value['fsName']})
    return status


def _parseVolumeStatusClients(tree):
    status = {'name': tree.find('volStatus/volumes/volume/volName').text,
              'bricks': []}
    for el in tree.findall('volStatus/volumes/volume/node'):
        hostname = el.find('hostname').text
        path = el.find('path').text
        hostuuid = el.find('peerid').text

        clientsStatus = []
        for c in el.findall('clientsStatus/client'):
            clientValue = {}
            for ch in c.getchildren():
                clientValue[ch.tag] = ch.text or ''
            clientsStatus.append({'hostname': clientValue['hostname'],
                                  'bytesRead': clientValue['bytesRead'],
                                  'bytesWrite': clientValue['bytesWrite']})

        status['bricks'].append({'brick': '%s:%s' % (hostname, path),
                                 'hostuuid': hostuuid,
                                 'clientsStatus': clientsStatus})
    return status


def _parseVolumeStatusMem(tree):
    status = {'name': tree.find('volStatus/volumes/volume/volName').text,
              'bricks': []}
    for el in tree.findall('volStatus/volumes/volume/node'):
        brick = {'brick': '%s:%s' % (el.find('hostname').text,
                                     el.find('path').text),
                 'hostuuid': el.find('peerid').text,
                 'mallinfo': {},
                 'mempool': []}

        for ch in el.find('memStatus/mallinfo').getchildren():
            brick['mallinfo'][ch.tag] = ch.text or ''

        for c in el.findall('memStatus/mempool/pool'):
            mempool = {}
            for ch in c.getchildren():
                mempool[ch.tag] = ch.text or ''
            brick['mempool'].append(mempool)

        status['bricks'].append(brick)
    return status


def volumeStatus(volumeName, brick=None, option=None):
    command = _getGlusterVolCmd() + ["status", volumeName]
    if brick:
        command.append(brick)
    if option:
        command.append(option)

    xmltree = _execGlusterXml(command)

    try:
        if option == 'detail':
            return _parseVolumeStatusDetail(xmltree)
        elif option == 'clients':
            return _parseVolumeStatusClients(xmltree)
        elif option == 'mem':
            return _parseVolumeStatusMem(xmltree)
        else:
            return _parseVolumeStatus(xmltree)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def _parseVolumeInfo(tree):
    volumes = {}
    for el in tree.findall('volInfo/volumes/volume'):
        value = {}
        value['volumeName'] = el.find('name').text
        value['uuid'] = el.find('id').text
        value['volumeType'] = el.find('typeStr').text.upper().replace('-', '_')
        status = el.find('statusStr').text.upper()
        if status == 'STARTED':
            value["volumeStatus"] = VolumeStatus.ONLINE
        else:
            value["volumeStatus"] = VolumeStatus.OFFLINE
        value['brickCount'] = el.find('brickCount').text
        value['distCount'] = el.find('distCount').text
        value['stripeCount'] = el.find('stripeCount').text
        value['replicaCount'] = el.find('replicaCount').text
        transportType = el.find('transport').text
        if transportType == '0':
            value['transportType'] = [TransportType.TCP]
        elif transportType == '1':
            value['transportType'] = [TransportType.RDMA]
        else:
            value['transportType'] = [TransportType.TCP, TransportType.RDMA]
        value['bricks'] = []
        value['options'] = {}
        value['bricksInfo'] = []
        for b in el.findall('bricks/brick'):
            value['bricks'].append(b.text)
        for o in el.findall('options/option'):
            value['options'][o.find('name').text] = o.find('value').text
        for d in el.findall('bricks/brick'):
            brickDetail = {}
            # this try block is to maintain backward compatibility
            # it returns an empty list when gluster doesnot return uuid
            try:
                brickDetail['name'] = d.find('name').text
                brickDetail['hostUuid'] = d.find('hostUuid').text
                value['bricksInfo'].append(brickDetail)
            except AttributeError:
                break
        volumes[value['volumeName']] = value
    return volumes


def _parseVolumeProfileInfo(tree, nfs):
    bricks = []
    if nfs:
        brickKey = 'nfs'
        bricksKey = 'nfsServers'
    else:
        brickKey = 'brick'
        bricksKey = 'bricks'
    for brick in tree.findall('volProfile/brick'):
        fopCumulative = []
        blkCumulative = []
        fopInterval = []
        blkInterval = []
        brickName = brick.find('brickName').text
        if brickName == 'localhost':
            brickName = _getLocalPeer()
        for block in brick.findall('cumulativeStats/blockStats/block'):
            blkCumulative.append({'size': block.find('size').text,
                                  'read': block.find('reads').text,
                                  'write': block.find('writes').text})
        for fop in brick.findall('cumulativeStats/fopStats/fop'):
            fopCumulative.append({'name': fop.find('name').text,
                                  'hits': fop.find('hits').text,
                                  'latencyAvg': fop.find('avgLatency').text,
                                  'latencyMin': fop.find('minLatency').text,
                                  'latencyMax': fop.find('maxLatency').text})
        for block in brick.findall('intervalStats/blockStats/block'):
            blkInterval.append({'size': block.find('size').text,
                                'read': block.find('reads').text,
                                'write': block.find('writes').text})
        for fop in brick.findall('intervalStats/fopStats/fop'):
            fopInterval.append({'name': fop.find('name').text,
                                'hits': fop.find('hits').text,
                                'latencyAvg': fop.find('avgLatency').text,
                                'latencyMin': fop.find('minLatency').text,
                                'latencyMax': fop.find('maxLatency').text})
        bricks.append(
            {brickKey: brickName,
             'cumulativeStats': {
                 'blockStats': blkCumulative,
                 'fopStats': fopCumulative,
                 'duration': brick.find('cumulativeStats/duration').text,
                 'totalRead': brick.find('cumulativeStats/totalRead').text,
                 'totalWrite': brick.find('cumulativeStats/totalWrite').text},
             'intervalStats': {
                 'blockStats': blkInterval,
                 'fopStats': fopInterval,
                 'duration': brick.find('intervalStats/duration').text,
                 'totalRead': brick.find('intervalStats/totalRead').text,
                 'totalWrite': brick.find('intervalStats/totalWrite').text}})
    status = {'volumeName': tree.find("volProfile/volname").text,
              bricksKey: bricks}
    return status


def volumeInfo(volumeName=None, remoteServer=None):
    command = _getGlusterVolCmd() + ["info"]
    if remoteServer:
        command += ['--remote-host=%s' % remoteServer]
    if volumeName:
        command.append(volumeName)

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeInfo(xmltree)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeCreate(volumeName, brickList, replicaCount=0, stripeCount=0,
                 transportList=[], force=False):
    command = _getGlusterVolCmd() + ["create", volumeName]
    if stripeCount:
        command += ["stripe", "%s" % stripeCount]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    if transportList:
        command += ["transport", ','.join(transportList)]
    command += brickList

    if force:
        command.append('force')

    xmltree = _execGlusterXml(command)

    try:
        return {'uuid': xmltree.find('volCreate/volume/id').text}
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeStart(volumeName, force=False):
    command = _getGlusterVolCmd() + ["start", volumeName]
    if force:
        command.append('force')

    _execGluster(command)
    return True


def volumeStop(volumeName, force=False):
    command = _getGlusterVolCmd() + ["stop", volumeName]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeDelete(volumeName):
    command = _getGlusterVolCmd() + ["delete", volumeName]

    _execGlusterXml(command)
    return True


def volumeSet(volumeName, option, value):
    command = _getGlusterVolCmd() + ["set", volumeName, option, value]

    _execGlusterXml(command)
    return True


def _parseVolumeSetHelpXml(out):
    optionList = []
    tree = etree.fromstring(out)
    for el in tree.findall('option'):
        option = {}
        for ch in el.getchildren():
            option[ch.tag] = ch.text or ''
        optionList.append(option)
    return optionList


def volumeSetHelpXml():
    rc, out, err = _execGluster(_getGlusterVolCmd() + ["set", 'help-xml'])
    return _parseVolumeSetHelpXml(out)


def volumeReset(volumeName, option='', force=False):
    command = _getGlusterVolCmd() + ['reset', volumeName]
    if option:
        command.append(option)
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeAddBrick(volumeName, brickList,
                   replicaCount=0, stripeCount=0, force=False):
    command = _getGlusterVolCmd() + ["add-brick", volumeName]
    if stripeCount:
        command += ["stripe", "%s" % stripeCount]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeRebalanceStart(volumeName, rebalanceType="", force=False):
    command = _getGlusterVolCmd() + ["rebalance", volumeName]
    if rebalanceType:
        command.append(rebalanceType)
    command.append("start")
    if force:
        command.append("force")

    xmltree = _execGlusterXml(command)

    try:
        return {'taskId': xmltree.find('volRebalance/task-id').text}
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeRebalanceStop(volumeName, force=False):
    command = _getGlusterVolCmd() + ["rebalance", volumeName, "stop"]
    if force:
        command.append('force')

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeRebalanceRemoveBrickStatus(xmltree, 'rebalance')
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def _parseVolumeRebalanceRemoveBrickStatus(xmltree, mode):
    if mode == 'rebalance':
        tree = xmltree.find('volRebalance')
    elif mode == 'remove-brick':
        tree = xmltree.find('volRemoveBrick')
    else:
        return

    st = tree.find('aggregate/statusStr').text
    statusStr = st.replace(' ', '_').replace('-', '_')
    status = {
        'summary': {
            'runtime': tree.find('aggregate/runtime').text,
            'filesScanned': tree.find('aggregate/lookups').text,
            'filesMoved': tree.find('aggregate/files').text,
            'filesFailed': tree.find('aggregate/failures').text,
            'filesSkipped': tree.find('aggregate/skipped').text,
            'totalSizeMoved': tree.find('aggregate/size').text,
            'status': statusStr.upper()},
        'hosts': []}

    for el in tree.findall('node'):
        st = el.find('statusStr').text
        statusStr = st.replace(' ', '_').replace('-', '_')
        status['hosts'].append({'name': el.find('nodeName').text,
                                'id': el.find('id').text,
                                'runtime': el.find('runtime').text,
                                'filesScanned': el.find('lookups').text,
                                'filesMoved': el.find('files').text,
                                'filesFailed': el.find('failures').text,
                                'filesSkipped': el.find('skipped').text,
                                'totalSizeMoved': el.find('size').text,
                                'status': statusStr.upper()})

    return status


def volumeRebalanceStatus(volumeName):
    command = _getGlusterVolCmd() + ["rebalance", volumeName, "status"]

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeRebalanceRemoveBrickStatus(xmltree, 'rebalance')
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeReplaceBrickStart(volumeName, existingBrick, newBrick):
    command = _getGlusterVolCmd() + ["replace-brick", volumeName,
                                     existingBrick, newBrick, "start"]

    xmltree = _execGlusterXml(command)

    try:
        return {'taskId': xmltree.find('volReplaceBrick/task-id').text}
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeReplaceBrickAbort(volumeName, existingBrick, newBrick):
    command = _getGlusterVolCmd() + ["replace-brick", volumeName,
                                     existingBrick, newBrick, "abort"]

    _execGlusterXml(command)
    return True


def volumeReplaceBrickPause(volumeName, existingBrick, newBrick):
    command = _getGlusterVolCmd() + ["replace-brick", volumeName,
                                     existingBrick, newBrick, "pause"]

    _execGlusterXml(command)
    return True


def volumeReplaceBrickStatus(volumeName, existingBrick, newBrick):
    rc, out, err = _execGluster(_getGlusterVolCmd() + ["replace-brick",
                                                       volumeName,
                                                       existingBrick, newBrick,
                                                       "status"])

    message = "\n".join(out)
    statLine = out[0].strip().upper()
    if BrickStatus.PAUSED in statLine:
        return BrickStatus.PAUSED, message
    elif statLine.endswith('MIGRATION COMPLETE'):
        return BrickStatus.COMPLETED, message
    elif statLine.startswith('NUMBER OF FILES MIGRATED'):
        return BrickStatus.RUNNING, message
    elif statLine.endswith("UNKNOWN"):
        return BrickStatus.UNKNOWN, message
    else:
        return BrickStatus.NA, message


def volumeReplaceBrickCommit(volumeName, existingBrick, newBrick,
                             force=False):
    command = _getGlusterVolCmd() + ["replace-brick", volumeName,
                                     existingBrick, newBrick, "commit"]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeRemoveBrickStart(volumeName, brickList, replicaCount=0):
    command = _getGlusterVolCmd() + ["remove-brick", volumeName]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList + ["start"]

    xmltree = _execGlusterXml(command)

    try:
        return {'taskId': xmltree.find('volRemoveBrick/task-id').text}
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeRemoveBrickStop(volumeName, brickList, replicaCount=0):
    command = _getGlusterVolCmd() + ["remove-brick", volumeName]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList + ["stop"]

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeRebalanceRemoveBrickStatus(xmltree, 'remove-brick')
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeRemoveBrickStatus(volumeName, brickList, replicaCount=0):
    command = _getGlusterVolCmd() + ["remove-brick", volumeName]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList + ["status"]

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeRebalanceRemoveBrickStatus(xmltree, 'remove-brick')
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeRemoveBrickCommit(volumeName, brickList, replicaCount=0):
    command = _getGlusterVolCmd() + ["remove-brick", volumeName]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList + ["commit"]

    _execGlusterXml(command)
    return True


def volumeRemoveBrickForce(volumeName, brickList, replicaCount=0):
    command = _getGlusterVolCmd() + ["remove-brick", volumeName]
    if replicaCount:
        command += ["replica", "%s" % replicaCount]
    command += brickList + ["force"]

    _execGlusterXml(command)
    return True


def peerProbe(hostName):
    command = _getGlusterPeerCmd() + ["probe", hostName]

    _execGlusterXml(command)
    return True


def peerDetach(hostName, force=False):
    command = _getGlusterPeerCmd() + ["detach", hostName]
    if force:
        command.append('force')

    try:
        _execGlusterXml(command)
        return True
    except GlusterCmdFailed as e:
        if e.rc == 2:
            raise GlusterPeerNotFound(hostName)
        raise


def _parsePeerStatus(tree, gHostName, gUuid, gStatus):
    hostList = [{'hostname': gHostName,
                 'uuid': gUuid,
                 'status': gStatus}]

    for el in tree.findall('peerStatus/peer'):
        if el.find('state').text != '3':
            status = HostStatus.UNKNOWN
        elif el.find('connected').text == '1':
            status = HostStatus.CONNECTED
        else:
            status = HostStatus.DISCONNECTED
        hostList.append({'hostname': el.find('hostname').text,
                         'uuid': el.find('uuid').text,
                         'status': status})

    return hostList


def peerStatus():
    command = _getGlusterPeerCmd() + ["status"]

    xmltree = _execGlusterXml(command)

    try:
        return _parsePeerStatus(xmltree,
                                _getLocalPeer(),
                                _getLocalPeerUUID(), HostStatus.CONNECTED)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeProfileStart(volumeName):
    command = _getGlusterVolCmd() + ["profile", volumeName, "start"]

    _execGlusterXml(command)
    return True


def volumeProfileStop(volumeName):
    command = _getGlusterVolCmd() + ["profile", volumeName, "stop"]

    _execGlusterXml(command)
    return True


def volumeProfileInfo(volumeName, nfs=False):
    command = _getGlusterVolCmd() + ["profile", volumeName, "info"]
    if nfs:
        command += ["nfs"]

    xmltree = _execGlusterXml(command)

    try:
        return _parseVolumeProfileInfo(xmltree, nfs)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def _parseVolumeTasks(tree):
    tasks = {}
    for el in tree.findall('volStatus/volumes/volume'):
        volumeName = el.find('volName').text
        for c in el.findall('tasks/task'):
            taskType = c.find('type').text
            taskType = taskType.upper().replace('-', '_').replace(' ', '_')
            taskId = c.find('id').text
            bricks = []
            if taskType == TaskType.REPLACE_BRICK:
                bricks.append(c.find('params/srcBrick').text)
                bricks.append(c.find('params/dstBrick').text)
            elif taskType == TaskType.REMOVE_BRICK:
                for b in c.findall('params/brick'):
                    bricks.append(b.text)
            elif taskType == TaskType.REBALANCE:
                pass

            statusStr = c.find('statusStr').text.upper() \
                                                .replace('-', '_') \
                                                .replace(' ', '_')

            tasks[taskId] = {'volumeName': volumeName,
                             'taskType': taskType,
                             'status': statusStr,
                             'bricks': bricks}
    return tasks


def volumeTasks(volumeName="all"):
    command = _getGlusterVolCmd() + ["status", volumeName, "tasks"]

    xmltree = _execGlusterXml(command)
    try:
        return _parseVolumeTasks(xmltree)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeGeoRepSessionStart(volumeName, remoteHost, remoteVolumeName,
                             force=False):
    command = _getGlusterVolGeoRepCmd() + [volumeName, "%s::%s" % (
        remoteHost, remoteVolumeName), "start"]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeGeoRepSessionStop(volumeName, remoteHost, remoteVolumeName,
                            force=False):
    command = _getGlusterVolGeoRepCmd() + [volumeName, "%s::%s" % (
        remoteHost, remoteVolumeName), "stop"]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def _parseGeoRepStatus(tree, detail=False):
    status = {}
    for volume in tree.findall('geoRep/volume'):
        sessions = []
        volumeDetail = {}
        for session in volume.findall('sessions/session'):
            pairs = []
            sessionDetail = {}
            sessionDetail['sessionKey'] = session.find('session_slave').text
            sessionDetail['remoteVolumeName'] = sessionDetail[
                'sessionKey'].split("::")[-1]
            for pair in session.findall('pair'):
                pairDetail = {}
                pairDetail['host'] = pair.find('master_node').text
                pairDetail['hostUuid'] = pair.find(
                    'master_node_uuid').text
                pairDetail['brickName'] = pair.find('master_brick').text
                pairDetail['remoteHost'] = pair.find(
                    'slave').text.split("::")[0]
                pairDetail['status'] = pair.find('status').text
                pairDetail['checkpointStatus'] = pair.find(
                    'checkpoint_status').text
                pairDetail['crawlStatus'] = pair.find('crawl_status').text
                if detail:
                    pairDetail['filesSynced'] = pair.find('files_syncd').text
                    pairDetail['filesPending'] = pair.find(
                        'files_pending').text
                    pairDetail['bytesPending'] = pair.find(
                        'bytes_pending').text
                    pairDetail['deletesPending'] = pair.find(
                        'deletes_pending').text
                    pairDetail['filesSkipped'] = pair.find(
                        'files_skipped').text
                pairs.append(pairDetail)
            sessionDetail['bricks'] = pairs
            sessions.append(sessionDetail)
        volumeDetail['sessions'] = sessions
        status[volume.find('name').text] = volumeDetail
    return status


def volumeGeoRepStatus(volumeName=None, remoteHost=None,
                       remoteVolumeName=None, detail=False):
    command = _getGlusterVolGeoRepCmd()
    if volumeName:
        command.append(volumeName)
    if remoteHost and remoteVolumeName:
        command.append("%s::%s" % (remoteHost, remoteVolumeName))
    command.append("status")
    if detail:
        command.append("detail")

    xmltree = _execGlusterXml(command)

    try:
        return _parseGeoRepStatus(xmltree, detail)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def volumeGeoRepSessionPause(volumeName, remoteHost, remoteVolumeName,
                             force=False):
    command = _getGlusterVolGeoRepCmd() + [volumeName, "%s::%s" % (
        remoteHost, remoteVolumeName), "pause"]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def volumeGeoRepSessionResume(volumeName, remoteHost, remoteVolumeName,
                              force=False):
    command = _getGlusterVolGeoRepCmd() + [volumeName, "%s::%s" % (
        remoteHost, remoteVolumeName), "resume"]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def _parseVolumeGeoRepConfig(tree):
    conf = tree.find('geoRep/config')
    config = {}
    for child in conf.getchildren():
        config[child.tag] = child.text
    return {'geoRepConfig': config}


def volumeGeoRepConfig(volumeName, remoteHost,
                       remoteVolumeName, optionName=None,
                       optionValue=None):
    command = _getGlusterVolGeoRepCmd() + [volumeName, "%s::%s" % (
        remoteHost, remoteVolumeName), "config"]
    if optionName and optionValue:
        command += [optionName, optionValue]
    elif optionName:
        command += ["!%s" % optionName]

    xmltree = _execGlusterXml(command)
    if optionName:
        return True

    try:
        return _parseVolumeGeoRepConfig(xmltree)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def snapshotCreate(volumeName, snapName,
                   snapDescription=None,
                   force=False):
    command = _getGlusterSnapshotCmd() + ["create", snapName, volumeName]

    if snapDescription:
        command += ['description', snapDescription]
    if force:
        command.append('force')

    xmltree = _execGlusterXml(command)

    try:
        return {'uuid': xmltree.find('snapCreate/snapshot/uuid').text}
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))


def snapshotDelete(volumeName=None, snapName=None):
    command = _getGlusterSnapshotCmd() + ["delete"]
    if snapName:
        command.append(snapName)
    elif volumeName:
        command += ["volume", volumeName]

    # xml output not used because of BZ:1161416 in gluster cli
    rc, out, err = _execGluster(command)
    return True


def snapshotActivate(snapName, force=False):
    command = _getGlusterSnapshotCmd() + ["activate", snapName]
    if force:
        command.append('force')

    _execGlusterXml(command)
    return True


def snapshotDeactivate(snapName):
    command = _getGlusterSnapshotCmd() + ["deactivate", snapName]

    _execGlusterXml(command)
    return True


def _parseRestoredSnapshot(tree):
    snapshotRestore = {}
    snapshotRestore['volumeName'] = tree.find('snapRestore/volume/name').text
    snapshotRestore['volumeUuid'] = tree.find('snapRestore/volume/uuid').text
    snapshotRestore['snapshotName'] = tree.find(
        'snapRestore/snapshot/name').text
    snapshotRestore['snapshotUuid'] = tree.find(
        'snapRestore/snapshot/uuid').text

    return snapshotRestore


def snapshotRestore(snapName):
    command = _getGlusterSnapshotCmd() + ["restore", snapName]

    xmltree = _execGlusterXml(command)

    try:
        return _parseRestoredSnapshot(xmltree)
    except _etreeExceptions:
        raise GlusterXMLError(command, etree.tostring(xmltree))
